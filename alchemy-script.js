const config  = require('config');
const mariadb = require("mariadb");
const Web3 = require("web3");

const web3 = new Web3(config.get('rpc_url'));

const big = web3.utils.toBN;
const toHex = web3.utils.toHex;

const EXTERNAL = "external";
const INTERNAL = "internal";
const TOKEN = "erc20";
const DEFAULT_UUID = "DEFAULT_UUID";

// Constants to differentiate between movement of assets.
const ASSET_MOVEMENT = ["In", "Out"];

// Constants to differentiate between type of assets.
const ASSET_TYPES = ["ETH", "ERC20"];


/**
 * @dev Establishes connection with database.
 *
 */
const pool = mariadb.createPool(config.get('db'));

const transfers_table = config.get('dbtables.transfers');
const sync_table = config.get('dbtables.sync');

var txCount = 0;

/**
 * @dev The main execution point of the script.
 *
 * It receives the list of addresses from command line arguments
 * and take their corresponding synced block when their balances
 * were last fetched and passes this data to {processAccounts}
 * function to get the transfers against each provided account
 * till the {latest} block.
 *
 * The retrieved transfers against list of addresses are stored in
 * {transfers_table} table in the database. The block that the balances are
 * fetched till is stored in {sync_table} table against the
 * account.
 */
async function main() {
    const accounts = config.get('accounts');
    accounts.forEach(
        (account) => {
            if (!web3.utils.isAddress(account)) {
                console.error("Invalid ETH Address: " + account);
                process.exit(1);
            }
        }
    );

    const toBlock = await web3.eth.getBlockNumber();
    const toBlockHex = toHex(toBlock).toString();
    console.log("Processing the history up to block " + toBlock);

    const accountsAndLastFetchedBlocks = await getLastFetchedBlockForAccounts(accounts);
    const data = await processAccounts(
        accountsAndLastFetchedBlocks,
        toBlockHex
    );
    console.log("Retrieved " + txCount + " transactions");
    try {
        // store all transfer data in database.
        await populateDatabase(data);

        // update the latest fetched for each of the accounts that are processed.
        await updateLatestFetchedBlockForAccounts(
            accounts,
            toBlock
        );
        process.exit(0);
    } catch (e) {
        console.error(e.message);
        process.exit(1);
    }
}

/**
 * @dev Iterates over all the provided accounts and call {fetchTransfers}
 * to get the transfers against accounts. An account with large list of
 * transfers requires multiple calls to {fetchTransfers} as the results
 * returned by {alchemy_getAssetTransfers} API returns finite number of
 * results and require pagination when the dataset is large.
 *
 * @param accountsAndLastFetchedBlocks An object represent each account
 * and its corresponding last fetched block.
 * @param toBlock The Block to fetch the transfers till.
 * @returns {Promise<unknown>} Object containing fetched and constructed transfers.
 */
async function processAccounts(
    accountsAndLastFetchedBlocks,
    toBlock
) {
    return Object.keys(accountsAndLastFetchedBlocks).reduce(
        async (
            acc,
            account,
        ) => {
            return new Promise(
                async (
                    resolve,
                    _
                ) => {
                    const [
                        processedTransfersFrom,
                        processedTransfersTo
                    ] = await Promise.all([
                        new Promise(async (resolve, _) => {
                                let uuid = DEFAULT_UUID;
                                let processedTransfersFrom = {};
                                while (uuid) {
                                    let transfersFrom;
                                    let pageKey;

                                    while (!transfersFrom) {
                                        const response = await fetchTransfers({
                                            fromBlock: accountsAndLastFetchedBlocks[account],
                                            toBlock,
                                            fromAddress: account,
                                            uuid,
                                        });

                                        if (response.result) {
                                            ({
                                                result: {
                                                    transfers: transfersFrom,
                                                    pageKey
                                                }
                                            } = response);
                                        }
                                    }

                                    uuid = pageKey;
                                    processedTransfersFrom = {
                                        ...processedTransfersFrom,
                                        ...(await processTransfers(transfersFrom, true))
                                    };
                                }
                                resolve(processedTransfersFrom);
                            }
                        ),
                        new Promise(async (resolve, _) => {
                            let uuid = DEFAULT_UUID;
                            let processedTransfersTo = {};
                            while (uuid) {
                                let transfersTo;
                                let pageKey;

                                while (!transfersTo) {
                                    const response = await fetchTransfers({
                                        fromBlock: accountsAndLastFetchedBlocks[account],
                                        toBlock,
                                        toAddress: account,
                                        uuid,
                                    });

                                    if (response.result) {
                                        ({
                                            result: {
                                                transfers: transfersTo,
                                                    pageKey,
                                            }
                                        } = response);
                                    }
                                }

                                uuid = pageKey;
                                processedTransfersTo = {
                                    ...processedTransfersTo,
                                    ...(await processTransfers(transfersTo)),
                                }
                            }
                            resolve(processedTransfersTo);
                        })
                    ]);

                    acc = await acc;
                    acc[account] = {
                        Out: {
                            ETH: processedTransfersFrom.ETH,
                            ERC20: processedTransfersFrom.ERC20,
                        },
                        In: {
                            ETH: processedTransfersTo.ETH,
                            ERC20: processedTransfersTo.ERC20,
                        }
                    };
                    resolve(acc);
                });

        },
        Promise.resolve({})
    );
}

/**
 * @dev Construct Transfers object specific to ERC20/ETH taking
 * into account if the transfer results in gas cost for the user.
 *
 * @param transfers List of transfers (either ETH/ERC20).
 * @param includeGasCost Are the transfers going out of account?
 * if so, they accrue gas cost for the involved account.
 * @returns {Promise} Returns transfer object.
 */
function processTransfers(
    transfers,
    includeGasCost = false
) {
    return transfers.reduce(
        async (
            acc,
            {
                blockNum,
                hash: txHash,
                to,
                from,
                asset: name,
                category,
                rawContract: {
                    value,
                    address,
                    decimal,
                }
            }
        ) => {
            if (
                category === EXTERNAL
                || category === INTERNAL
                || category === TOKEN
            ) {
                //console.log("TxHash: ", txHash);
                txCount++;

                const transferData = {
                    txHash,
                    from,
                    to,
                    name,
                    value: big(value).toString(),
                    address: category === TOKEN
                        ? address
                        : "none",
                    decimal: !!decimal
                        ? big(decimal).toString()
                        : "none",
                    blockNum: big(blockNum).toString(),
                    timestamp: (await web3.eth.getBlock(big(blockNum))).timestamp,
                };

                if (includeGasCost) {
                    const {
                        from: txSender,
                        gasUsed,
                        effectiveGasPrice
                    } = await web3.eth.getTransactionReceipt(txHash);
                    if (
                        txSender === from
                        && category !== TOKEN
                    ) {
                        const gasPriceToUse = effectiveGasPrice
                            ? effectiveGasPrice
                            : (await web3.eth.getTransaction(txHash)).gasPrice;

                        transferData.gasCostInWei = big(gasUsed)
                            .mul(big(gasPriceToUse))
                            .toString();
                    }
                }

                const asset = category === EXTERNAL || category === INTERNAL
                    ? "ETH"
                    : "ERC20";

                acc = await acc;
                if (!acc[asset]) acc[asset] = [transferData];
                else acc[asset].push(transferData);

                return {
                    ...acc,
                };
            } else {
                throw new Error("Unsupported category");
            }
        }, Promise.resolve({})
    );
}

/**
 * @dev Generates payload to send to {alchemy_getAssetTransfers} API.
 */
function generatePayload(params) {
    // {DEFAULT_UUID} means no pagination is needed.
    if (params.uuid === DEFAULT_UUID) delete params.uuid;

    return {
        method: "alchemy_getAssetTransfers",
        id: 1,
        jsonrpc: "2.0",
        params: [{
            excludeZeroValue: false,
            category: [
                "external",
                "internal",
                "erc20"
            ],
            ...params,
        }],
    }
}

/**
 * @dev Performs RPC call to web3 provider.
 */
async function fetchTransfers(params) {
    return new Promise((resolve, reject) => {
        web3.currentProvider.send(
            generatePayload(params), (
                err,
                res
            ) => {
                if (err) reject(err)
                resolve(res);
            });
    });
}

/**
 * @dev Updates the last fetched block for user in the database.
 * @param accounts List of involved accounts.
 * @param lastFetchedBlockNum The block number to update with.
 */
async function updateLatestFetchedBlockForAccounts(
    accounts,
    lastFetchedBlockNum
) {
    const conn = await pool.getConnection();
    await conn.batch(
        "REPLACE INTO " + sync_table + " (" +
        "address," +
        " blockNum" +
        ")" +
        " VALUES (?, ?)",
        accounts.map(
            account => [account.toLowerCase(), lastFetchedBlockNum])
    );
    await conn.commit();
    await conn.release();
    await conn.end();
}

/**
 * @dev Construct an object containing accounts and their last fetched blocks.
 * @param accounts List of accounts
 */
async function getLastFetchedBlockForAccounts(accounts) {
    const conn = await pool.getConnection();
    const accountsAndLastFetchedBlocks = await accounts.reduce(
        async (
            acc,
            account
        ) => {
            const blockNum = await conn.query(
                "SELECT blockNum from " + sync_table + " where address = "
                + `'${account.toLowerCase()}'`
            );
            acc = await acc;
            return {
                ...acc,
                [account]: toHex(
                    blockNum.length ? blockNum[0].blockNum : 0
                ).toString(),
            }
        }, Promise.resolve({})
    );
    await conn.release();
    return accountsAndLastFetchedBlocks;
}

/**
 * @dev Construct an array by restructuring dataset to update database in batch
 * instead of inserting each Transfer entry individually.
 *
 * @param data Dataset of all transfers constructed during the current run of script.
 */
function aggregateData(data) {
    return ASSET_MOVEMENT.reduce(
        (
            acc,
            movement
        ) => {
            return [
                ...acc,
                ...ASSET_TYPES.reduce(
                    (
                        acc,
                        type
                    ) => {
                        if (!data[movement][type])
                            return acc;

                        return [
                            ...acc,
                            ...(data[movement][type].map(
                                ({
                                     txHash,
                                     from,
                                     to,
                                     name,
                                     value,
                                     address,
                                     decimal,
                                     blockNum,
                                     timestamp,
                                     gasCostInWei,
                                 }) => [
                                    txHash,
                                    from,
                                    to,
                                    name,
                                    value,
                                    address,
                                    decimal,
                                    blockNum,
                                    timestamp,
                                    gasCostInWei ? gasCostInWei : "none",
                                ]
                            ))
                        ]
                    }, []
                )
            ];
        }, []
    );
}

/**
 * @dev Updates the database with the constructed and fetched transfers data
 * against provided list of accounts.
 *
 * @param data Aggregated dataset to insert into database.
 */
async function populateDatabase(data) {
    const conn = await pool.getConnection();
    await Object.keys(data).reduce(
        async (
            acc,
            account,
        ) => {
            const aggregatedData = aggregateData(data[account]);
            if (aggregatedData.length) {
                await conn.batch(
                    "INSERT INTO " + transfers_table + " (" +
                    "txHash," +
                    " fromAddress," +
                    " toAddress," +
                    " assetName," +
                    " value," +
                    " contractAddress," +
                    " assetDecimal," +
                    " blockNum," +
                    " timestamp," +
                    " gasCostInWei" +
                    ") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    aggregatedData
                );
                await conn.commit();
            }
            await acc;
        }, Promise.resolve()
    );
    await conn.release();
}

/**
 * @dev Starts the script.
 */
main();

