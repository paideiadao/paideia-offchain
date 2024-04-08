import asyncio
from datetime import datetime
from distutils import extension
import json
from pydoc import text
import signal
import sys
import time
from ergo_python_appkit.appkit import ErgoAppKit
import logging
import requests
from kafka import KafkaConsumer, KafkaProducer
import threading
import os

from staking.StakingState import StakingState
from paideia_contracts.contracts.staking import (
    StakingConfig,
    PaideiaConfig,
    EGIOConfig,
    ergopadv5testConfig,
    NETAConfig,
    AHTConfig,
    Ergo_Crux_LPConfig,
)
import java
from org.ergoplatform.appkit import UnsignedTransaction, SignedTransaction
from org.ergoplatform.appkit.impl import BlockchainContextImpl
from java.lang.reflect import UndeclaredThrowableException
from jpype import JImplements, JOverride

levelname = logging.DEBUG
logging.basicConfig(
    format="{asctime}:{name:>8s}:{levelname:<8s}::{message}", style="{", level=levelname
)
logger = logging.getLogger("kafka")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)
logger = logging.getLogger("urllib3.connectionpool")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)

producer: KafkaProducer = None

project = os.getenv("PROJECT")

stakingConfigs = {
    "im.paideia": PaideiaConfig,
    "io.ergogames": EGIOConfig,
    "im.paideia-testnet": ergopadv5testConfig,
    "io.anetabtc": NETAConfig,
    "org.ergoauctions": AHTConfig,
    "io.cruxfinance": Ergo_Crux_LPConfig,
}


async def getConfig():
    logging.info("Retrieving config...")
    configResult = requests.get("http://eo-api:8901/api/config")

    return configResult.json()


async def initiateFilters(stakingConfig: StakingConfig):
    logging.info("Initiating filters...")
    topics = [
        f"{project}.staking.emit_time",
        f"{project}.staking.mempoolcheck",
        f"{project}.staking.shutdown",
    ]

    with open("staking/filters/tx_stake_state.json") as f:
        stake_state_filter = json.loads(f.read())
        stake_state_filter["filterTree"][
            "comparisonValue"
        ] = stakingConfig.stakeStateNFT
        stake_state_filter["name"] = project + stake_state_filter["name"]
        stake_state_filter["topics"][0] = project + stake_state_filter["topics"][0]
    res = requests.post("http://eo-api:8901/api/filter", json=stake_state_filter)
    if res.ok:
        logging.debug(res.json())
        topics = topics + stake_state_filter["topics"]
    else:
        logging.error(res.content)

    with open("staking/filters/tx_emission.json") as f:
        emission_filter = json.loads(f.read())
        emission_filter["filterTree"]["comparisonValue"] = stakingConfig.emissionNFT
        emission_filter["name"] = project + emission_filter["name"]
        emission_filter["topics"][0] = project + emission_filter["topics"][0]
    res = requests.post("http://eo-api:8901/api/filter", json=emission_filter)
    if res.ok:
        logging.debug(res.json())
        topics = topics + emission_filter["topics"]

    with open("staking/filters/tx_incentive.json") as f:
        incentive_filter = json.loads(f.read())
        incentive_filter["filterTree"][
            "comparisonValue"
        ] = stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
        incentive_filter["name"] = project + incentive_filter["name"]
        incentive_filter["topics"][0] = project + incentive_filter["topics"][0]
    res = requests.post("http://eo-api:8901/api/filter", json=incentive_filter)
    if res.ok:
        logging.debug(res.json())
        topics = topics + incentive_filter["topics"]

    with open("staking/filters/tx_proxy.json") as f:
        proxy_filter = json.loads(f.read())
        stakeProxyFilter = {
            "nodeType": "equals",
            "fieldName": "outputs.0.ergoTree",
            "comparisonValue": stakingConfig.stakeProxyContract._ergoTree.bytesHex(),
        }
        proxy_filter["filterTree"]["childNodes"].append(stakeProxyFilter)
        addStakeProxyFilter = {
            "nodeType": "equals",
            "fieldName": "outputs.0.ergoTree",
            "comparisonValue": stakingConfig.addStakeProxyContract._ergoTree.bytesHex(),
        }
        proxy_filter["filterTree"]["childNodes"].append(addStakeProxyFilter)
        unstakeProxyFilter = {
            "nodeType": "equals",
            "fieldName": "outputs.0.ergoTree",
            "comparisonValue": stakingConfig.unstakeProxyContract._ergoTree.bytesHex(),
        }
        proxy_filter["filterTree"]["childNodes"].append(unstakeProxyFilter)
        proxy_filter["name"] = project + proxy_filter["name"]
        proxy_filter["topics"][0] = project + proxy_filter["topics"][0]
    res = requests.post("http://eo-api:8901/api/filter", json=proxy_filter)
    if res.ok:
        logging.debug(res.json())
        topics = topics + proxy_filter["topics"]

    with open("staking/filters/tx_refund.json") as f:
        refund_filter = json.loads(f.read())
        stakeProxyRefundFilter = {
            "nodeType": "equals",
            "fieldName": "inputs.0.address",
            "comparisonValue": stakingConfig.stakeProxyContract.contract.toAddress().toString(),
        }
        refund_filter["filterTree"]["childNodes"].append(stakeProxyRefundFilter)
        addStakeProxyRefundFilter = {
            "nodeType": "equals",
            "fieldName": "inputs.0.address",
            "comparisonValue": stakingConfig.addStakeProxyContract.contract.toAddress().toString(),
        }
        refund_filter["filterTree"]["childNodes"].append(addStakeProxyRefundFilter)
        unstakeProxyRefundFilter = {
            "nodeType": "equals",
            "fieldName": "inputs.0.address",
            "comparisonValue": stakingConfig.unstakeProxyContract.contract.toAddress().toString(),
        }
        refund_filter["filterTree"]["childNodes"].append(unstakeProxyRefundFilter)
        refund_filter["name"] = project + refund_filter["name"]
        refund_filter["topics"][0] = project + refund_filter["topics"][0]
    res = requests.post("http://eo-api:8901/api/filter", json=refund_filter)
    if res.ok:
        logging.debug(res.json())
        topics = topics + refund_filter["topics"]

    return topics


async def currentStakingState(config, stakingConfig: StakingConfig) -> StakingState:
    logging.info("Fetching current staking state...")
    result = StakingState(stakingConfig)

    logging.info("Finding stake state box...")
    res = requests.get(
        f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakingConfig.stakeStateNFT}',
        timeout=120,
    )
    if res.ok:
        result.stakeState = res.json()["items"][0]
    logging.debug(result.nextCycleTime())

    await setTimeFilter(result)

    logging.info("Finding emission box...")
    res = requests.get(
        f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakingConfig.emissionNFT}',
        timeout=120,
    )
    if res.ok:
        result.emission = res.json()["items"][0]

    logging.info("Finding stake pool box...")
    res = requests.get(
        f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakingConfig.stakePoolNFT}',
        timeout=120,
    )
    if res.ok:
        result.stakePool = res.json()["items"][0]

    logging.info("Finding stake boxes...")
    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        success = False
        while not success:
            try:
                req = f'{config["ERGO_NODE"]}/blockchain/box/unspent/byAddress?offset={offset}&limit={limit}&sortDirection=asc'
                logging.debug(req)
                res = requests.post(
                    req,
                    data=stakingConfig.stakeContract.contract.toAddress().toString(),
                    timeout=120,
                )
                if res.ok:
                    success = True
            except Exception as e:
                logging.error(f"currentStakingState::{e}")
                pass
        boxes = res.json()
        moreBoxes = len(boxes) == limit
        for box in boxes:
            if box["assets"][0]["tokenId"] == stakingConfig.stakeTokenId:
                result.addStakeBox(box)
            else:
                logging.warning(
                    f"{box['assets'][0]['tokenId']} did not match {stakingConfig.stakedTokenId}"
                )
        offset += limit

    logging.info("Finding incentive boxes...")
    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(
            f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.stakingIncentiveContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',
            timeout=120,
        )
        if res.ok:
            boxes = res.json()["items"]
            moreBoxes = len(boxes) == limit
            for box in boxes:
                result.addIncentiveBox(box)
            offset += limit
        else:
            moreBoxes = False

    logging.info("Finding stake proxy boxes...")
    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(
            f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.stakeProxyContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',
            timeout=120,
        )
        if res.ok:
            boxes = res.json()["items"]
            moreBoxes = len(boxes) == limit
            for box in boxes:
                result.addProxyBox(box)
            offset += limit
        else:
            moreBoxes = False

    logging.info("Finding add stake proxy boxes...")
    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(
            f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.addStakeProxyContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',
            timeout=120,
        )
        if res.ok:
            boxes = res.json()["items"]
            moreBoxes = len(boxes) == limit
            for box in boxes:
                result.addProxyBox(box)
            offset += limit
        else:
            moreBoxes = False

    logging.info("Finding unstake proxy boxes...")
    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(
            f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.unstakeProxyContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',
            timeout=120,
        )
        if res.ok:
            boxes = res.json()["items"]
            moreBoxes = len(boxes) == limit
            for box in boxes:
                result.addProxyBox(box)
            offset += limit
        else:
            moreBoxes = False

    return result


async def setTimeFilter(stakingState: StakingState):
    logging.info(
        f"Setting filter for next emission: {datetime.fromtimestamp(stakingState.nextCycleTime()/1000).isoformat()}"
    )
    with open("staking/filters/block_emit_time.json") as f:
        block_time_filter = json.loads(f.read())
        block_time_filter["name"] = project + block_time_filter["name"]
        block_time_filter["topics"][0] = project + block_time_filter["topics"][0]
    block_time_filter["filterTree"]["comparisonValue"] = stakingState.nextCycleTime()
    res = requests.post("http://eo-api:8901/api/filter", json=block_time_filter)


@JImplements(java.util.function.Function)
class SignedTxFromJsonExecutor(object):
    def __init__(self, signedTxJson: str):
        self._signedTxJson = signedTxJson

    @JOverride
    def apply(self, ctx: BlockchainContextImpl) -> SignedTransaction:
        return ctx.signedTxFromJson(self._signedTxJson)


def signedTxFromJson(appKit: ErgoAppKit, signedTxJson: str) -> SignedTransaction:
    return appKit._ergoClient.execute(SignedTxFromJsonExecutor(signedTxJson))


def dummySign(unsignedJson):
    for inp in unsignedJson["inputs"]:
        inp["spendingProof"] = {"proofBytes": "", "extension": {}}
    for outp in unsignedJson["outputs"]:
        outp["value"] = int(outp["value"])
        if "assets" in outp:
            for asset in outp["assets"]:
                asset["amount"] = int(asset["amount"])
    return unsignedJson


async def makeTx(
    appKit: ErgoAppKit, stakingState: StakingState, config, producer: KafkaProducer
):
    unsignedTx = None
    txType = ""
    try:
        unsignedTx = stakingState.emitTransaction(appKit, config["REWARD_ADDRESS"])
        if unsignedTx is not None:
            txType = f"{project}.staking.emit"
    except Exception as e:
        logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.compoundTX(appKit, config["REWARD_ADDRESS"])
            if unsignedTx is not None:
                txType = f"{project}.staking.compound"
        except Exception as e:
            logging.error(e)
    if unsignedTx is None:
        try:
            (txType, unsignedTx) = stakingState.proxyTransaction(
                appKit, config["REWARD_ADDRESS"]
            )
        except Exception as e:
            logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.consolidateTransaction(
                appKit, config["REWARD_ADDRESS"]
            )
            if unsignedTx is not None:
                txType = f"{project}.staking.consolidate"
                logging.info("Submitting consolidate tx")
        except Exception as e:
            logging.error(e)
    if unsignedTx is not None:
        try:
            signedTx = None
            if txType in [
                f"{project}.staking.emit",
                f"{project}.staking.compound",
                f"{project}.staking.proxy.add",
            ]:
                signedTxJson = dummySign(ErgoAppKit.unsignedTxToJson(unsignedTx))
            else:
                signedTx = appKit.signTransaction(unsignedTx)
                signedTxJson = json.loads(signedTx.toJson(False))
                stakingState.newTx(signedTxJson)
            txInfo = {"type": txType, "tx": signedTxJson}
            producer.send("ergo.submit_tx", value=txInfo)
            if signedTx is not None:
                logging.info(f"Submitting {txType} tx, cost: {signedTx.getCost()}")
            else:
                logging.info(f"Submitting {txType} tx, cost: unknown")
                # signedTxJson["id"] = "3"
                # stakingState.newTx(signedTxJson)
        except Exception as e:
            logging.info(ErgoAppKit.unsignedTxToJson(unsignedTx))
            logging.error(e)


async def checkMempool(config):
    producer = None
    start_time = time.time()
    current_time = start_time
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",
                value_serializer=lambda m: json.dumps(m).encode("utf-8"),
            )
        except:
            await asyncio.sleep(2)
    while current_time - start_time < 3600:
        await asyncio.sleep(240)
        current_time = time.time()
        try:
            producer.send(f"{project}.staking.mempoolcheck", "{'dummy':1}")
        except Exception as e:
            logging.error(e)
    try:
        producer.send(f"{project}.staking.shutdown", "{'dummy':1}")
    except Exception as e:
        logging.error(e)


async def main():
    try:
        config = await getConfig()
        threading.Thread(target=asyncio.run, args=(checkMempool(config),)).start()
        appKit = ErgoAppKit(config["ERGO_NODE"], "mainnet", config["ERGO_EXPLORER"])
        stakingConfig = stakingConfigs[project](appKit)
        try:
            topics = await initiateFilters(stakingConfig)
        except Exception as e:
            logging.error(e)
            return
        stakingState = await currentStakingState(config, stakingConfig)
        logging.info(stakingState)
        consumer = KafkaConsumer(
            *topics,
            session_timeout_ms=60000,
            group_id=f"{project}.staking",
            bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        producer = None
        while producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",
                    value_serializer=lambda m: json.dumps(m).encode("utf-8"),
                )
            except:
                await asyncio.sleep(2)
        await makeTx(appKit, stakingState, config, producer)
        for message in consumer:
            logging.info(message.topic)
            logging.info(message.value)
            if message.topic == f"{project}.staking.shutdown":
                logging.info("Shutting down consumer")
                consumer.close()
                logging.info("Killing parent")
                os.kill(os.getppid(), signal.SIGTERM)
                logging.info("Exiting process")
                return 1
            if message.topic == f"{project}.staking.refund":
                tx = message.value
                stakingState.newTx(tx)
            if message.topic == f"{project}.staking.proxy":
                tx = message.value
                stakingState.newTx(tx)
            if message.topic == f"{project}.staking.mempoolcheck":
                logging.info("Checking mempool")
                stakingState.mempool.validateMempool(config["ERGO_NODE"])
            if message.topic == f"{project}.staking.stake_state":
                tx = message.value
                stakingState.newTx(tx)
                if "globalIndex" in tx:
                    for outp in tx["outputs"]:
                        if (
                            outp["ergoTree"]
                            == stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
                        ):
                            stakingState.addIncentiveBox(outp)
                    stakingState.stakeState = tx["outputs"][0]
                    if (
                        tx["outputs"][1]["assets"][0]["tokenId"]
                        == stakingConfig.stakeTokenId
                    ):
                        if (
                            tx["inputs"][1]["assets"][0]["tokenId"]
                            == stakingConfig.stakeTokenId
                        ):
                            logging.info("Add stake transaction")
                            stakingState.removeProxyBox(tx["inputs"][2]["boxId"])
                        else:
                            logging.info("Stake transaction")
                            stakingState.removeProxyBox(tx["inputs"][1]["boxId"])
                        stakingState.addStakeBox(tx["outputs"][1])
                    else:
                        if (
                            tx["outputs"][1]["assets"][0]["tokenId"]
                            == stakingConfig.stakePoolNFT
                        ):
                            logging.info("Emit transaction")
                            stakingState.stakePool = tx["outputs"][1]
                            stakingState.emission = tx["outputs"][2]
                            await setTimeFilter(stakingState)
                        else:
                            stakingState.removeProxyBox(tx["inputs"][2]["boxId"])
                            if len(tx["outputs"][2]["additionalRegisters"]) > 0:
                                logging.info("Partial Unstake transaction")
                                stakingState.addStakeBox(tx["outputs"][2])
                            else:
                                logging.info("Unstake transaction")
                                stakingState.removeStakeBox(tx["inputs"][1]["boxId"])
            if message.topic == f"{project}.staking.emission":
                tx = message.value
                stakingState.newTx(tx)
                if "globalIndex" in tx:
                    logging.info("Compound transaction")
                    stakingState.emission = tx["outputs"][0]
                    for outp in tx["outputs"]:
                        if len(outp["assets"]) > 0:
                            if (
                                outp["assets"][0]["tokenId"]
                                == stakingConfig.stakeTokenId
                            ):
                                stakingState.addStakeBox(outp)
                        if (
                            outp["ergoTree"]
                            == stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
                        ):
                            stakingState.addIncentiveBox(outp)
            if message.topic == f"{project}.staking.incentive":
                tx = message.value
                stakingState.newTx(tx)
                if "globalIndex" in tx:
                    for outp in tx["outputs"]:
                        if (
                            outp["ergoTree"]
                            == stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
                        ):
                            stakingState.addIncentiveBox(outp)
                    logging.info("Funding or consolidation transaction")
            if message.topic == f"{project}.staking.emit_time":
                logging.info("Emission time")
            logging.info(stakingState)
            await makeTx(appKit, stakingState, config, producer)
    except Exception as e:
        logging.error(e)


asyncio.run(main())
