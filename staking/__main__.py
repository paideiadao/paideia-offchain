import asyncio
from distutils import extension
import json
from pydoc import text
import sys
import time
from ergo_python_appkit.appkit import ErgoAppKit
import logging
import requests
from kafka import KafkaConsumer, KafkaProducer
import threading

from staking.StakingState import StakingState
from paideia_contracts.contracts.staking import StakingConfig, PaideiaConfig
import java
from org.ergoplatform.appkit import UnsignedTransaction, SignedTransaction
from org.ergoplatform.appkit.impl import BlockchainContextImpl
from java.lang.reflect import UndeclaredThrowableException
from jpype import JImplements, JOverride

levelname = logging.DEBUG
logging.basicConfig(format='{asctime}:{name:>8s}:{levelname:<8s}::{message}', style='{', level=levelname)
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)
logger = logging.getLogger('urllib3.connectionpool')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)

producer: KafkaProducer = None

async def getConfig():
    configResult = requests.get("http://eo-api:8901/api/config")

    return configResult.json()

async def initiateFilters(stakingConfig: StakingConfig):
    topics = ['im.paideia.staking.emit_time','im.paideia.staking.mempoolcheck']

    with open('staking/filters/tx_stake_state.json') as f:
        stake_state_filter = json.loads(f.read())
        stake_state_filter["filterTree"]["comparisonValue"] = stakingConfig.stakeStateNFT
    res = requests.post('http://eo-api:8901/api/filter', json=stake_state_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + stake_state_filter['topics']
    else:
        logging.error(res.content)

    with open('staking/filters/tx_emission.json') as f:
        emission_filter = json.loads(f.read())
        emission_filter["filterTree"]["comparisonValue"] = stakingConfig.emissionNFT
    res = requests.post('http://eo-api:8901/api/filter', json=emission_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + emission_filter['topics']

    with open('staking/filters/tx_incentive.json') as f:
        incentive_filter = json.loads(f.read())
        incentive_filter["filterTree"]["comparisonValue"] = stakingConfig.stakingIncentiveContract._ergoTree.bytesHex()
    res = requests.post('http://eo-api:8901/api/filter', json=incentive_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + incentive_filter['topics']

    with open('staking/filters/tx_proxy.json') as f:
        proxy_filter = json.loads(f.read())
        stakeProxyFilter = {'nodeType': 'equals','fieldName': 'outputs.0.ergoTree','comparisonValue': stakingConfig.stakeProxyContract._ergoTree.bytesHex()}
        proxy_filter["filterTree"]["childNodes"].append(stakeProxyFilter)
        addStakeProxyFilter = {'nodeType': 'equals','fieldName': 'outputs.0.ergoTree','comparisonValue': stakingConfig.addStakeProxyContract._ergoTree.bytesHex()}
        proxy_filter["filterTree"]["childNodes"].append(addStakeProxyFilter)
        unstakeProxyFilter = {'nodeType': 'equals','fieldName': 'outputs.0.ergoTree','comparisonValue': stakingConfig.unstakeProxyContract._ergoTree.bytesHex()}
        proxy_filter["filterTree"]["childNodes"].append(unstakeProxyFilter)
    res = requests.post('http://eo-api:8901/api/filter', json=proxy_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + proxy_filter['topics']
    
    return topics

async def currentStakingState(config, stakingConfig: StakingConfig) -> StakingState:
    result = StakingState(stakingConfig)

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakingConfig.stakeStateNFT}',timeout=120)
    if res.ok:
        result.stakeState = res.json()["items"][0]
    logging.info(result.nextCycleTime())

    await setTimeFilter(result)

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakingConfig.emissionNFT}',timeout=120)
    if res.ok:
        result.emission = res.json()["items"][0]

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakingConfig.stakePoolNFT}',timeout=120)
    if res.ok:
        result.stakePool = res.json()["items"][0]

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        success = False
        while not success:
            try:
                res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakingConfig.stakeTokenId}?offset={offset}&limit={limit}',timeout=120)
                success = True
            except:
                pass
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            if box["assets"][0]["tokenId"] == stakingConfig.stakeTokenId:
                result.addStakeBox(box)
        offset += limit

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.stakingIncentiveContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',timeout=120)
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            result.addIncentiveBox(box)
        offset += limit

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.stakeProxyContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',timeout=120)
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            result.addProxyBox(box)
        offset += limit

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.addStakeProxyContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',timeout=120)
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            result.addProxyBox(box)
        offset += limit

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{stakingConfig.unstakeProxyContract.contract.toAddress().toString()}?offset={offset}&limit={limit}',timeout=120)
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            result.addProxyBox(box)
        offset += limit

    return result

async def setTimeFilter(stakingState: StakingState):
    with open('staking/filters/block_emit_time.json') as f:
        block_time_filter = json.loads(f.read())
    block_time_filter["filterTree"]["comparisonValue"] = stakingState.nextCycleTime()
    res = requests.post('http://eo-api:8901/api/filter', json=block_time_filter)

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

async def makeTx(appKit: ErgoAppKit, stakingState: StakingState, config, producer: KafkaProducer):
    unsignedTx = None
    txType = ""
    try:
        unsignedTx = stakingState.emitTransaction(appKit,config['REWARD_ADDRESS'])
        if unsignedTx is not None:
            txType = "im.paideia.staking.emit"
    except Exception as e:
        logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.compoundTX(appKit,config['REWARD_ADDRESS'])
            if unsignedTx is not None:
                txType = "im.paideia.staking.compound"
        except Exception as e:
            pass#logging.error(e)
    if unsignedTx is None:
        try:
            (txType, unsignedTx) = stakingState.proxyTransaction(appKit,config['REWARD_ADDRESS']) 
        except Exception as e:
            logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.consolidateTransaction(appKit,config['REWARD_ADDRESS'])
            if unsignedTx is not None:
                txType = "im.paideia.staking.consolidate"
                logging.info("Submitting consolidate tx")
        except Exception as e:
            logging.error(e)
    if unsignedTx is not None:
        try:
            if txType in ["im.paideia.staking.emit","im.paideia.staking.compound","im.paideia.staking.proxy.add"]:
                signedTxJson = dummySign(ErgoAppKit.unsignedTxToJson(unsignedTx))
            else:
                signedTx = appKit.signTransaction(unsignedTx)
                signedTxJson = json.loads(signedTx.toJson(False))
                stakingState.newTx(signedTxJson)
            txInfo = {'type': txType, 'tx': signedTxJson}
            producer.send('ergo.submit_tx',value=txInfo)
            logging.info(f"Submitting {txType} tx")
        except Exception as e:
            logging.error(e)

async def checkMempool(config):
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        except:
            await asyncio.sleep(2)
    while True:
        await asyncio.sleep(240)
        try:
            producer.send('im.paideia.staking.mempoolcheck',"{'dummy':1}")
        except Exception as e:
            logging.error(e)

async def main():
    try:
        config = await getConfig()
        threading.Thread(target=asyncio.run, args=(checkMempool(config),)).start()
        appKit = ErgoAppKit(config['ERGO_NODE'],'mainnet',config['ERGO_EXPLORER'])

        stakingConfig = PaideiaConfig(appKit)
        try:
            topics = await initiateFilters(stakingConfig)
        except Exception as e:
            logging.error(e)
            return
        stakingState = await currentStakingState(config,stakingConfig)
        logging.info(stakingState._stakeBoxes)
        logging.info(stakingState)
        consumer = KafkaConsumer(*topics,group_id='im.paideia.staking',bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        producer = None
        while producer is None:
            try:
                producer = KafkaProducer(bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_serializer=lambda m: json.dumps(m).encode('utf-8'))
            except:
                await asyncio.sleep(2)
        await makeTx(appKit,stakingState,config,producer)
        for message in consumer:
            logging.info(message.topic)
            logging.info(message.value)
            if message.topic == "im.paideia.staking.proxy":
                tx = message.value
                stakingState.newTx(tx)
            if message.topic == "im.paideia.staking.mempoolcheck":
                logging.info("Checking mempool")
                stakingState.mempool.validateMempool(config['ERGO_NODE'])
            if message.topic == "im.paideia.staking.stake_state":
                tx = message.value
                stakingState.newTx(tx)
                if "globalIndex" in tx:
                    for outp in tx["outputs"]:
                        if outp["ergoTree"] == stakingConfig.stakingIncentiveContract._ergoTree.bytesHex():
                            stakingState.addIncentiveBox(outp)
                    stakingState.stakeState = tx["outputs"][0]
                    if tx["outputs"][1]["assets"][0]["tokenId"] == stakingConfig.stakeTokenId:
                        if tx["inputs"][1]["assets"][0]["tokenId"] == stakingConfig.stakeTokenId:
                            logging.info("Add stake transaction")
                            stakingState.removeProxyBox(tx["inputs"][2]["boxId"])
                        else:
                            logging.info("Stake transaction")
                            stakingState.removeProxyBox(tx["inputs"][1]["boxId"])
                        stakingState.addStakeBox(tx["outputs"][1])
                    else:
                        if tx["outputs"][1]["assets"][0]["tokenId"] == stakingConfig.stakePoolNFT:
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
            if message.topic == "im.paideia.staking.emission":
                tx = message.value
                stakingState.newTx(tx)
                if "globalIndex" in tx:
                    logging.info("Compound transaction")
                    stakingState.emission = tx["outputs"][0]
                    for outp in tx["outputs"]:
                        if len(outp["assets"]) > 0:
                            if outp["assets"][0]["tokenId"] == stakingConfig.stakeTokenId:
                                stakingState.addStakeBox(outp)
                        if outp["ergoTree"] == stakingConfig.stakingIncentiveContract._ergoTree.bytesHex():
                            stakingState.addIncentiveBox(outp)
            if message.topic == "im.paideia.staking.incentive":
                tx = message.value
                stakingState.newTx(tx)
                if "globalIndex" in tx:
                    for outp in tx["outputs"]:
                        if outp["ergoTree"] == stakingConfig.stakingIncentiveContract._ergoTree.bytesHex():
                            stakingState.addIncentiveBox(outp)
                    logging.info("Funding or consolidation transaction")
            if message.topic == "im.paideia.staking.emit_time":
                logging.info("Emission time")
            logging.info(stakingState)
            await makeTx(appKit,stakingState,config,producer)
    except Exception as e:
        logging.error(e)
asyncio.run(main())

            