import os
from time import time
from typing import List
import logging
from utils.Mempool import Mempool
from ergo_python_appkit.appkit import ErgoAppKit
from ergo_python_appkit.ErgoTransaction import ErgoTransaction
from ergo_python_appkit.ErgoBox import ErgoBox
from org.ergoplatform.appkit import ErgoValue
from paideia_contracts.contracts.staking import (
    AddStakeTransaction,
    StakingConfig,
    CompoundTransaction,
    EmitTransaction,
    StakeTransaction,
    UnstakeTransaction,
    ConsolidateDustTransaction,
    StakePoolBox
)

import java.lang.IllegalArgumentException


class StakingState:
    def __init__(self, stakingConfig: StakingConfig) -> None:
        self._stakeBoxes = {}
        self._incentiveBoxes = {}
        self.mempool: Mempool = Mempool()
        self.stakingConfig = stakingConfig
        self._proxyBoxes = {}
        self.project = os.getenv("PROJECT")

    def getR4(self, box):
        hexVal = box["additionalRegisters"]["R4"]
        return ErgoValue.fromHex(hexVal).getValue()

    def nextCycleTime(self):
        r4 = self.getR4(self.stakeState)
        return r4.apply(3) + r4.apply(4)

    def addStakeBox(self, stakeBox) -> bool:
        mempool = "inclusionHeight" not in stakeBox
        if not mempool:
            r5 = stakeBox["additionalRegisters"]["R5"][4:]
            if r5 in self._stakeBoxes:
                if stakeBox["inclusionHeight"] <= self._stakeBoxes[r5]["inclusionHeight"]:
                    return False
            self._stakeBoxes[r5] = stakeBox
            return True
        else:
            False

    def removeStakeBox(self, stakeBoxId):
        keyToRemove = None
        for stakeBox in self._stakeBoxes.keys():
            if self._stakeBoxes[stakeBox]["boxId"] == stakeBoxId:
                keyToRemove = stakeBox
        if keyToRemove is not None:
            self._stakeBoxes.pop(keyToRemove, None)

    def addIncentiveBox(self, incentiveBox) -> bool:
        mempool = "inclusionHeight" not in incentiveBox
        if not mempool and incentiveBox["spentTransactionId"] is None:
            self._incentiveBoxes[incentiveBox["boxId"]] = incentiveBox
            return True
        return False

    def removeIncentiveBox(self, incentiveBoxId):
        self._incentiveBoxes.pop(incentiveBoxId, None)

    def getIncentiveBox(self, value: int):
        for box in list(self._incentiveBoxes.values()) + self.mempool.getUTXOsByTree(
            self.stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
        ):
            if (
                not self.mempool.isSpent(box["boxId"])
                and box["value"] > value + 1000000
            ):
                return box

    def incentiveTotal(self):
        total = 0
        for box in list(self._incentiveBoxes.values()) + self.mempool.getUTXOsByTree(
            self.stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
        ):
            if not self.mempool.isSpent(box["boxId"]):
                total += box["value"]
        return total

    def addProxyBox(self, proxyBox) -> bool:
        mempool = "inclusionHeight" not in proxyBox
        if not mempool and "R4" in proxyBox["additionalRegisters"]:
            self._proxyBoxes[proxyBox["boxId"]] = proxyBox
            return True
        return False

    def removeProxyBox(self, proxyBoxId):
        self._proxyBoxes.pop(proxyBoxId, None)

    def getProxyBox(self, proxyTree: str):
        proxyBoxList = list(self._proxyBoxes.values()) + self.mempool.getUTXOsByTree(
            proxyTree
        )
        if proxyBoxList is not None:
            for box in proxyBoxList:
                if (
                    not self.mempool.isSpent(box["boxId"])
                    and box["ergoTree"] == proxyTree
                    and "R4" in box["additionalRegisters"]
                ):
                    return box

    def getProxyBoxById(self, boxId):
        box = self.mempool.getBoxById(boxId)
        if box is not None:
            return box
        return self._proxyBoxes[boxId]

    def newTx(self, tx):
        isMempool = "globalIndex" not in tx
        if isMempool:
            self.mempool.addTx(tx)
        else:
            self.mempool.removeTx(tx["id"])
            for box in tx["inputs"]:
                self.removeIncentiveBox(box["boxId"])
                self.removeStakeBox(box["boxId"])
                self.removeProxyBox(box["boxId"])
            for box in tx["outputs"]:
                if (
                    box["ergoTree"]
                    == self.stakingConfig.stakeProxyContract._ergoTree.bytesHex()
                    or box["ergoTree"]
                    == self.stakingConfig.addStakeProxyContract._ergoTree.bytesHex()
                    or box["ergoTree"]
                    == self.stakingConfig.unstakeProxyContract._ergoTree.bytesHex()
                ):
                    self.addProxyBox(box)

    def __str__(self):
        result = "Current staking state:\n"
        result += f"Stake State: {self.stakeState['boxId']}\n"
        result += f"Emission: {self.emission['boxId']}\n"
        result += f"Stake Pool: {self.stakePool['boxId']}\n"
        result += f"Number of stake boxes: {len(self._stakeBoxes.keys())}\n"
        result += f"Incentive total: {self.incentiveTotal()}\n"
        result += f"Proxy boxes: {len(self._proxyBoxes.keys())}\n"
        return result

    def compoundTX(self, appKit: ErgoAppKit, rewardAddress: str):
        try:
            stakeBoxes = []

            # emmission box contains current staking info
            emissionR4 = self.getR4(self.emission)
            # emission box R4[2] contains current remaining stakers
            if emissionR4.apply(2) <= 0:
                # logging.info("Remaining stakers: 0")
                return

            for box in self.mempool.getUTXOsByTree(
                self.stakingConfig.stakeContract._ergoTree.bytesHex()
            ) + list(self._stakeBoxes.values()):
                boxR4 = self.getR4(box)
                if boxR4.apply(0) == emissionR4.apply(1) and not self.mempool.isSpent(
                    box["boxId"]
                ):
                    # calc rewards and build tx
                    stakeBoxes.append(box["boxId"])

                # every <numBoxes>, go ahead and submit tx
                if len(stakeBoxes) >= 50:
                    logging.debug("found 50")
                    break

            if len(stakeBoxes) == 0:
                return

            txValue = int(
                self.stakingConfig.baseCompoundMinerFee
                + self.stakingConfig.baseCompoundReward
                + (
                    (
                        self.stakingConfig.variableCompoundMinerFee
                        + self.stakingConfig.variableCompoundReward
                    )
                    * len(stakeBoxes)
                )
            )

            incentiveBox = self.getIncentiveBox(txValue)

            if incentiveBox is None:
                raise Exception("Not enough incentive for compound")

            emissionInput = appKit.getBoxesById([self.emission["boxId"]])[0]
            stakeInputs = appKit.getBoxesById(stakeBoxes)
            incentiveInput = appKit.getBoxesById([incentiveBox["boxId"]])[0]

            return CompoundTransaction(
                emissionInput,
                stakeInputs,
                incentiveInput,
                self.stakingConfig,
                rewardAddress,
            ).unsignedTx

        except Exception as e:
            logging.error(f"ERR:{e}")

    def emitTransaction(self, appKit: ErgoAppKit, rewardAddress: str):
        if self.nextCycleTime() < time() * 1000:
            stakeStateInput = appKit.getBoxesById([self.stakeState["boxId"]])[0]
            stakePoolInput = appKit.getBoxesById([self.stakePool["boxId"]])[0]
            emissionInput = appKit.getBoxesById([self.emission["boxId"]])[0]

            stakePoolBox = StakePoolBox.fromInputBox(stakePoolInput, self.stakingConfig.stakePoolContract)

            if stakePoolBox.remaining <= stakePoolBox.emissionAmount:
                raise Exception("Not enough tokens in stakepool for emission")

            incentiveBox = self.getIncentiveBox(int(5e6))

            if incentiveBox is None:
                raise Exception("Not enough incentive for emit")

            incentiveInput = appKit.getBoxesById([incentiveBox["boxId"]])[0]

            emissionR4 = self.getR4(self.emission)
            if emissionR4.apply(2) > 0:
                raise Exception("Previous emit not finished yet")
            return EmitTransaction(
                stakeStateInput,
                stakePoolInput,
                emissionInput,
                incentiveInput,
                self.stakingConfig,
                rewardAddress,
            ).unsignedTx

    def getStakeBoxByKey(self, stakeKey: str):
        return self._stakeBoxes[stakeKey]

    def getRegisterHex(self, box, register):
        hexVal = box["additionalRegisters"][register][4:]
        return hexVal

    def proxyTransaction(self, appKit: ErgoAppKit, rewardAddress: str):
        proxy = self.getProxyBox(
            self.stakingConfig.stakeProxyContract._ergoTree.bytesHex()
        )
        if proxy is not None:
            stakeStateInput = appKit.getBoxesById([self.stakeState["boxId"]])[0]
            stakeProxyInput = appKit.getBoxesById([proxy["boxId"]])[0]
            if self.getR4(proxy).apply(0) < (time() - 1800) * 1000:
                userOutput = ErgoBox(
                    appKit=appKit,
                    value=stakeProxyInput.getValue() - int(1e6),
                    contract=appKit.contractFromTree(
                        appKit.treeFromBytes(
                            bytes.fromhex(self.getRegisterHex(proxy, "R5"))
                        )
                    ),
                    tokens={
                        stakeProxyInput.getTokens()[0]
                        .getId()
                        .toString(): stakeProxyInput.getTokens()[0]
                        .getValue()
                    },
                ).outBox
                tx = ErgoTransaction(appKit)
                tx.inputs = [stakeProxyInput]
                tx.outputs = [userOutput]
                tx.fee = int(1e6)
                tx.changeAddress = rewardAddress
                return (f"{self.project}.staking.proxy.refund", tx.unsignedTx)
            return (
                f"{self.project}.staking.proxy.new",
                StakeTransaction(
                    stakeStateInput, stakeProxyInput, self.stakingConfig, rewardAddress
                ).unsignedTx,
            )
        proxy = self.getProxyBox(
            self.stakingConfig.addStakeProxyContract._ergoTree.bytesHex()
        )
        if proxy is not None:
            stakeStateInput = appKit.getBoxesById([self.stakeState["boxId"]])[0]
            addStakeProxyInput = appKit.getBoxesById([proxy["boxId"]])[0]
            try:
                stakeInput = appKit.getBoxesById(
                    [
                        self.getStakeBoxByKey(
                            addStakeProxyInput.getTokens()[0].getId().toString()
                        )["boxId"]
                    ]
                )[0]
            except:
                userOutput = ErgoBox(
                    appKit=appKit,
                    value=addStakeProxyInput.getValue() - int(1e6),
                    contract=appKit.contractFromTree(
                        appKit.treeFromBytes(
                            bytes.fromhex(self.getRegisterHex(proxy, "R5"))
                        )
                    ),
                    tokens={
                        addStakeProxyInput.getTokens()[0]
                        .getId()
                        .toString(): addStakeProxyInput.getTokens()[0]
                        .getValue(),
                        addStakeProxyInput.getTokens()[1]
                        .getId()
                        .toString(): addStakeProxyInput.getTokens()[1]
                        .getValue(),
                    },
                ).outBox
                tx = ErgoTransaction(appKit)
                tx.inputs = [addStakeProxyInput]
                tx.outputs = [userOutput]
                tx.fee = int(1e6)
                tx.changeAddress = rewardAddress
                return (f"{self.project}.staking.proxy.refund", tx.unsignedTx)
            addStakeProxyTx = AddStakeTransaction(
                stakeStateInput,
                stakeInput,
                addStakeProxyInput,
                self.stakingConfig,
                rewardAddress,
            )
            logging.info(addStakeProxyTx.eip12)
            return (f"{self.project}.staking.proxy.add", addStakeProxyTx.unsignedTx)
        proxy = self.getProxyBox(
            self.stakingConfig.unstakeProxyContract._ergoTree.bytesHex()
        )
        if proxy is not None:
            unstakeProxyInput = appKit.getBoxesById([proxy["boxId"]])[0]
            try:
                stakeStateInput = appKit.getBoxesById([self.stakeState["boxId"]])[0]
                stakeInput = appKit.getBoxesById(
                    [
                        self.getStakeBoxByKey(
                            unstakeProxyInput.getTokens()[0].getId().toString()
                        )["boxId"]
                    ]
                )[0]
                remaining = stakeInput.getTokens()[
                    1
                ].getValue() - unstakeProxyInput.getRegisters()[0].getValue().apply(0)
                if remaining > 0 and remaining < 1000:
                    raise Exception("Not enough tokens remaining")
                return (
                    f"{self.project}.staking.proxy.remove",
                    UnstakeTransaction(
                        stakeStateInput,
                        stakeInput,
                        unstakeProxyInput,
                        self.stakingConfig,
                        rewardAddress,
                    ).unsignedTx,
                )
            except:
                logging.info(proxy)
                userOutput = ErgoBox(
                    appKit=appKit,
                    value=unstakeProxyInput.getValue() - int(1e6),
                    contract=appKit.contractFromTree(
                        appKit.treeFromBytes(
                            bytes.fromhex(self.getRegisterHex(proxy, "R5"))
                        )
                    ),
                    tokens={unstakeProxyInput.getTokens()[0].getId().toString(): 1},
                ).outBox
                tx = ErgoTransaction(appKit)
                tx.inputs = [unstakeProxyInput]
                tx.outputs = [userOutput]
                tx.fee = int(1e6)
                tx.changeAddress = rewardAddress
                return (f"{self.project}.staking.proxy.refund", tx.unsignedTx)

        return (None, None)

    def consolidateTransaction(self, appKit: ErgoAppKit, rewardAddres: str):
        dustBoxes = []
        dustTotal = 0
        for box in list(self._incentiveBoxes.values()) + self.mempool.getUTXOsByTree(
            self.stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
        ):
            if (
                box["boxId"] not in dustBoxes
                and box["value"] < 10000000
                and not self.mempool.isSpent(box["boxId"])
            ):
                dustBoxes.append(box["boxId"])
                dustTotal += box["value"]

        if len(dustBoxes) >= 2:
            logging.info(f"Found {len(dustBoxes)} dustboxes")
            inputs = appKit.getBoxesById(dustBoxes)

            consolidationTx = ConsolidateDustTransaction(
                inputs, self.stakingConfig, rewardAddres
            )

            return consolidationTx.unsignedTx

    @property
    def stakeState(self):
        if self.mempool.getUTXOByTokenId(self.stakingConfig.stakeStateNFT) is not None:
            return self.mempool.getUTXOByTokenId(self.stakingConfig.stakeStateNFT)
        return self._stakeState

    @stakeState.setter
    def stakeState(self, value):
        if "inclusionHeight" in value and value["spentTransactionId"] is None:
            self._stakeState = value

    @property
    def emission(self):
        if self.mempool.getUTXOByTokenId(self.stakingConfig.emissionNFT) is not None:
            return self.mempool.getUTXOByTokenId(self.stakingConfig.emissionNFT)
        return self._emission

    @emission.setter
    def emission(self, value):
        if "inclusionHeight" in value and value["spentTransactionId"] is None:
            self._emission = value

    @property
    def stakePool(self):
        if self.mempool.getUTXOByTokenId(self.stakingConfig.stakePoolNFT) is not None:
            return self.mempool.getUTXOByTokenId(self.stakingConfig.stakePoolNFT)
        return self._stakePool

    @stakePool.setter
    def stakePool(self, value):
        if "inclusionHeight" in value and value["spentTransactionId"] is None:
            self._stakePool = value

    @property
    def cycle(self) -> int:
        return self._cycle

    @cycle.setter
    def cycle(self, value: int):
        self._cycle = value
