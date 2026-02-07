import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import TronWeb from 'tronweb';
import Redis from 'ioredis';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function getEnvNumber(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return fallback;
    const value = Number(raw);
    return Number.isFinite(value) ? value : fallback;
}

function normalizeCsv(value) {
    return String(value)
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
}

function formatUnits(value, decimals) {
    const base = 10n ** BigInt(decimals);
    const integer = value / base;
    const fraction = value % base;
    if (fraction === 0n) return integer.toString();
    const paddedFraction = fraction.toString().padStart(decimals, '0');
    const trimmedFraction = paddedFraction.replace(/0+$/, '');
    return `${integer.toString()}.${trimmedFraction}`;
}

function parseUnits(value, decimals) {
    const trimmed = String(value).trim();
    if (trimmed === '') return 0n;
    if (!/^\d+(\.\d+)?$/.test(trimmed)) return 0n;
    const [integerPart, fractionPart = ''] = trimmed.split('.');
    const fraction = fractionPart.slice(0, decimals).padEnd(decimals, '0');
    return BigInt(integerPart) * 10n ** BigInt(decimals) + BigInt(fraction);
}

const tronFullHost = process.env.TRON_FULL_HOST ?? 'https://api.trongrid.io';
const tronGridApiKeys = normalizeCsv(process.env.TRON_PRO_API_KEYS ?? '');
const redisUrl = process.env.REDIS_URL ?? 'redis://localhost:6379/0';

const streamPrefix = process.env.REDIS_STREAM_PREFIX ?? 'stream:tron';
const trxStreamKey = `${streamPrefix}:trx`;
const trc20StreamKey = `${streamPrefix}:trc20`;

const stateFilePath = process.env.STATE_FILE_PATH ?? path.join(__dirname, 'lastBlockAll.json');
const pollIntervalMs = getEnvNumber('POLL_INTERVAL_MS', 1000);
const blockStepDelayMs = getEnvNumber('BLOCK_STEP_DELAY_MS', 200);

const trxDecimals = 6;
const defaultTrc20Decimals = 6;

const minTrxAmountSun = parseUnits(process.env.MIN_TRX_AMOUNT ?? '0.01', trxDecimals);
const minTrc20AmountBaseUnits = parseUnits(process.env.MIN_TRC20_AMOUNT ?? '0.01', defaultTrc20Decimals);

const trc20ContractWhitelist = new Set(
    normalizeCsv(
        process.env.TRC20_CONTRACTS ??
        'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t,TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8',
    ).map((address) => address.toLowerCase()),
);

const tokenMetaByContract = new Map([
    ['tr7nhqjekqxgtci8q8zy4pl8otszgjlj6t', { symbol: 'USDT', decimals: 6 }],
    ['tekxitehnzsmse2xqrbj4w32run966rdz8', { symbol: 'USDC', decimals: 6 }],
]);

const tronGridApiKeyUsage = new Map(tronGridApiKeys.map((key) => [key, 0]));

function pickTronGridApiKey() {
    if (tronGridApiKeys.length === 0) return undefined;
    let bestKey = tronGridApiKeys[0];
    let bestUsage = tronGridApiKeyUsage.get(bestKey) ?? 0;
    for (const key of tronGridApiKeys) {
        const usage = tronGridApiKeyUsage.get(key) ?? 0;
        if (usage < bestUsage) {
            bestUsage = usage;
            bestKey = key;
        }
    }
    tronGridApiKeyUsage.set(bestKey, bestUsage + 1);
    return bestKey;
}

function createTronWebClient() {
    const apiKey = pickTronGridApiKey();
    return new TronWeb({
        fullHost: tronFullHost,
        headers: apiKey ? { 'TRON-PRO-API-KEY': apiKey } : undefined,
    });
}

const redis = new Redis(redisUrl);

let lastScannedBlockHeight = 0;

function loadLastScannedHeight() {
    if (!fs.existsSync(stateFilePath)) return;
    try {
        const state = JSON.parse(fs.readFileSync(stateFilePath, 'utf8'));
        lastScannedBlockHeight = Number(state.lastScannedBlockHeight) || 0;
    } catch (error) {
        console.error('Failed to read state file:', error?.message ?? error);
    }
}

function saveLastScannedHeight(height) {
    fs.writeFileSync(stateFilePath, JSON.stringify({ lastScannedBlockHeight: height }, null, 2));
}

function decodeTrc20TransferInput(tronWeb, triggerValue) {
    const data = triggerValue?.data;
    if (!data || typeof data !== 'string') return null;

    const methodId = data.slice(0, 8);
    if (methodId !== 'a9059cbb') return null;
    if (data.length < 8 + 64 + 64) return null;

    const contractAddress = tronWeb.address.fromHex(triggerValue.contract_address);
    if (!trc20ContractWhitelist.has(contractAddress.toLowerCase())) return null;

    const toHex = `41${data.slice(8 + 24, 8 + 64)}`;
    const to = tronWeb.address.fromHex(toHex);

    const amountHex = data.slice(8 + 64, 8 + 128);
    const amountBaseUnits = BigInt(`0x${amountHex}`);

    const meta = tokenMetaByContract.get(contractAddress.toLowerCase()) ?? {
        symbol: 'TRC20',
        decimals: defaultTrc20Decimals,
    };

    return {
        contractAddress,
        tokenSymbol: meta.symbol,
        tokenDecimals: meta.decimals,
        to,
        amountBaseUnits,
        amount: formatUnits(amountBaseUnits, meta.decimals),
    };
}

async function scanBlock(height) {
    const tronWeb = createTronWebClient();
    try {
        const block = await tronWeb.trx.getBlock(height);
        const transactions = Array.isArray(block?.transactions) ? block.transactions : [];
        console.log(`Scanning block ${height} | transactions=${transactions.length}`);

        const trxTransfers = [];
        const trc20Transfers = [];

        for (const tx of transactions) {
            const txId = tx?.txID;
            const contractRet = tx?.ret?.[0]?.contractRet?.toUpperCase?.() ?? '';
            if (contractRet !== 'SUCCESS') continue;

            const contract = tx?.raw_data?.contract?.[0];
            const contractType = contract?.type;
            const value = contract?.parameter?.value;
            if (!contractType || !value) continue;

            if (contractType === 'TransferContract') {
                const from = tronWeb.address.fromHex(value.owner_address);
                const to = tronWeb.address.fromHex(value.to_address);
                const amountSun = BigInt(value.amount);
                if (amountSun < minTrxAmountSun) continue;

                trxTransfers.push({
                    tokenSymbol: 'TRX',
                    tokenDecimals: trxDecimals,
                    from,
                    to,
                    amount: formatUnits(amountSun, trxDecimals),
                    amountBaseUnits: amountSun.toString(),
                    txId,
                });
                continue;
            }

            if (contractType === 'TriggerSmartContract') {
                try {
                    const decoded = decodeTrc20TransferInput(tronWeb, value);
                    if (!decoded) continue;
                    if (decoded.amountBaseUnits < minTrc20AmountBaseUnits) continue;

                    const from = tronWeb.address.fromHex(value.owner_address);
                    trc20Transfers.push({
                        tokenSymbol: decoded.tokenSymbol,
                        tokenDecimals: decoded.tokenDecimals,
                        contractAddress: decoded.contractAddress,
                        from,
                        to: decoded.to,
                        amount: decoded.amount,
                        amountBaseUnits: decoded.amountBaseUnits.toString(),
                        txId,
                    });
                } catch (error) {
                    console.warn(`Failed to decode TRC20 transfer txId=${txId}:`, error?.message ?? error);
                }
            }
        }

        if (trxTransfers.length > 0) {
            const messageId = await redis.xadd(
                trxStreamKey,
                'MAXLEN',
                '~',
                500,
                '*',
                'block',
                String(height),
                'transactions',
                JSON.stringify(trxTransfers),
            );
            console.log(`Pushed TRX transfers | block=${height} | count=${trxTransfers.length} | redisId=${messageId}`);
        }

        if (trc20Transfers.length > 0) {
            const messageId = await redis.xadd(
                trc20StreamKey,
                'MAXLEN',
                '~',
                500,
                '*',
                'block',
                String(height),
                'transactions',
                JSON.stringify(trc20Transfers),
            );
            console.log(`Pushed TRC20 transfers | block=${height} | count=${trc20Transfers.length} | redisId=${messageId}`);
        }

        return true;
    } catch (error) {
        console.error(`Failed to fetch/scan block ${height}:`, error?.message ?? error);
        return false;
    }
}

async function runBlockScanner() {
    while (true) {
        try {
            const tronWeb = createTronWebClient();
            const latestBlock = await tronWeb.trx.getCurrentBlock();
            const latestHeight = latestBlock?.block_header?.raw_data?.number;

            if (typeof latestHeight !== 'number') {
                console.error('Unexpected getCurrentBlock() response; missing block height');
                await delay(pollIntervalMs);
                continue;
            }

            if (lastScannedBlockHeight === 0) {
                lastScannedBlockHeight = latestHeight - 1;
            }

            while (lastScannedBlockHeight < latestHeight) {
                const nextHeight = lastScannedBlockHeight + 1;
                const ok = await scanBlock(nextHeight);
                if (!ok) break;
                lastScannedBlockHeight = nextHeight;
                saveLastScannedHeight(lastScannedBlockHeight);
                await delay(blockStepDelayMs);
            }
        } catch (error) {
            console.error('Failed to poll latest block:', error?.message ?? error);
        }

        await delay(pollIntervalMs);
    }
}

process.on('SIGINT', async () => {
    try {
        await redis.quit();
    } finally {
        process.exit(0);
    }
});

loadLastScannedHeight();
runBlockScanner();

