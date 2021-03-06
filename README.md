# snowflake-ingest-node
simple API wrapper for Snowpipe for https://www.snowflake.com/ .  At time of writing only Python and Java were available SDKs

[![NPM version](http://img.shields.io/npm/v/snowflake-ingest-node.svg?style=flat-square)](https://www.npmjs.com/package/snowflake-ingest-node)
[![NPM downloads](http://img.shields.io/npm/dm/snowflake-ingest-node.svg?style=flat-square)](https://www.npmjs.com/package/snowflake-ingest-node)

Here is an example serverless project that uses snowpipe and CDC:
https://github.com/brianzinn/snowflake-cdc-example

```typescript
import * as dotenv from 'dotenv';

import { getLatestSecret } from '../src/SecretsManager';
import { createSnowpipeAPI, SnowpipeAPI, APIEndpointHistory } from 'snowflake-ingest-node';

describe(' > Snowflake API harness', () => {

    let snowpipeAPI: SnowpipeAPI;
    const getSnowpipeAPI = async (): Promise<SnowpipeAPI> => {
        const {
            SNOWFLAKE_USERNAME: username,
            SNOWFLAKE_REGION_ID: regionId,
            SNOWFLAKE_CLOUD_PROVIDER: cloudProvider,
            SNOWFLAKE_ACCOUNT: account,
            SNOWFLAKE_PRIVATE_KEY_NAME: privateKeyName
        } = process.env;
        const privateKey = await getLatestSecret(privateKeyName);
        const result = createSnowpipeAPI(username, privateKey, account, regionId, cloudProvider, {
            recordHistory: true
        });
        return result;
    };

    beforeEach(async () => {
        dotenv.config();
        snowflakeAPI = await getSnowflakeAPI();
    });

    // Case-sensitive, fully-qualified pipe name. For example, myDatabase.mySchema.myPipe.
    const PIPE_NAME = 'myDatabase.mySchema.myPipe';

    it.skip('insertFile', async () => {
        try {
            const response = await snowflakeAPI.insertFile(['/path/file-name.csv'], PIPE_NAME);
            console.log(response);
        } catch (e) {
            console.error(e);
        }
        const endpointHistory: APIEndpointHistory = snowpipeAPI.endpointHistory;
        console.log(endpointHistory.insertFile[0].response);
    });

    it.skip('insertReport', async () => {
        try {
            const response = await snowflakeAPI.insertReport(PIPE_NAME, '<unique-request-id-optional>');
            console.log(response);
        } catch (e) {
            console.error(e);
        }
        console.log(snowflakeAPI.endpointHistory().insertReport[0].response);
    });

    it.skip('loadHistoryScan', async () => {
        // start time is in ISO 8601 format zulu timezone.  Probably use a library like moment.tz.
        try {
            const response = await snowflakeAPI.loadHistoryScan(PIPE_NAME, '2020-10-14T02:00:00.000Z');
            console.log(response);
        } catch (e) {
            console.error(e);
        }
    })
});
```

> strongly typed Snowpipe API responses:
![Strongly Typed](https://github.com/brianzinn/snowflake-ingest-node/raw/main/images/strong-typed.png)

> runtime view (same as typings)
![Runtime Debug](https://github.com/brianzinn/snowflake-ingest-node/raw/main/images/runtime-debug.png)

snowpipe intro:
https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro.html

Note that there is also an auto-ingest feature built into snowflake.

# secrets and environment
You'll want to ensure your private key is secure in a vault or secret management (I am storing the lookup key as an environment variable). The rest could come from environment or hard coding.  Here is a sample `.env` as above for running locally and against your setup in the cloud likely serverless:
```bash
SNOWFLAKE_PRIVATE_KEY_NAME=<your key name in secrets manager>
SNOWFLAKE_ACCOUNT=<snowflake account>
SNOWFLAKE_USERNAME=<user you created with permission to pipe>
SNOWFLAKE_REGION_ID=<region is optional see docs ie: us-central1>
SNOWFLAKE_CLOUD_PROVIDER=<optional as well.  see docs: could be gcp (you can get this from your instance website full URL>
```

# adding to your project
You can just copy the one file or add via npm:
```
yarn add snowflake-ingest-node
yarn add jwt-simple
```
There is a peer dependency on `jwt-simple`, so make sure it is added as well.  There are no other dependencies except for built-in node (https and crypto) modules.
