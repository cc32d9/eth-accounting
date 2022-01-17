#Setup
1. Install project dependencies by running command.

```
npm install
```
2. Run an instance of `Mariadb` and create database and tables. Provide the database username according to your settings.

```
mysql ACCOUNTINGDB <eth-accounting-dbtables.sql
```

3. Create the configuration

```
mkdir config
cat >config/default.json <<'EOT'
{
    "db": {
      "host":       "localhost",
      "user":       "ACCOUNTINGUSER",
      "password":   "PASSWORD",
      "database":   "ACCOUNTINGDB"
    },
    "rpc_url": "https://eth-mainnet.alchemyapi.io/v2/SECRETTOKEN",
    "dbtables": {
      "transfers": "ETH_TRANSFERS",
      "sync": "ETH_ACCOUNTING_SYNC"
    },
    "accounts": [
      "0x5e624faedc7aa381b574c3c2ff1731677dd2ee1d",
      "0xaf648ffbc940570f3f6a9ca49b07ba7bc520bcdf"
    ]
}
EOT
```
4. Run the script

```
node alchemy-script.js
```