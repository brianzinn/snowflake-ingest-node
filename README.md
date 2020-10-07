# snowflake-ingest-node
simple API wrapper for Snowpipe.  At time of writing only Python and Java were available SDKs

```typescript
import * as dotenv from 'dotenv';

import { getLatestSecret } from '../src/SecretsManager';
import { APIEndpointHistory, createSnowpipeAPI } from 'snowflake-ingest-node';

describe(' > Snowflake API harness', () => {

    let snowflakeAPI: {
        loadHistoryScan: (pipeName: string, startTimeInclusive: string, endTimeExclusive?: string) => Promise<string>;
        insertReport: (pipeName: string, beginMark?: string) => Promise<string>;
        insertFile: (filenames: string[], pipeName: string) => Promise<string>;
        endpointHistory: () => APIEndpointHistory;
    };

    const getSnowflakeAPI = async () => {
        const {
            snowflake_username: username,
            snowflake_region_id: regionId,
            snowflake_cloud_provider: cloudProvider,
            snowflake_account: account,
            snowflake_private_key: privateKeyName
        } = process.env;
        const privateKey = await getLatestSecret(privateKeyName);
        const result = createSnowpipeAPI(username, privateKey, account, regionId, cloudProvider, {
            recordHistory: true
        });
        return result;
    }

    beforeEach(async () => {
        dotenv.config();
        snowflakeAPI = await getSnowflakeAPI();
    });

    // Case-sensitive, fully-qualified pipe name. For example, myDatabase.mySchema.myPipe.
    const PIPE_NAME = 'myDatabase.mySchema.myPipe';

    it.skip('insertFile', async () => {
        try {
            /* const response =*/ await snowflakeAPI.insertFile(['/transactions/1602017572486-0000-00-00:00:00.csv'], PIPE_NAME);
        } catch (e) {
            console.error(e);
        }
        console.log(snowflakeAPI.endpointHistory().insertFile[0].response);
    });

    it.skip('insertReport', async () => {
        try {
            /* const response = */ await snowflakeAPI.insertReport(PIPE_NAME, '33161bae019054d7aa5839ffbd60a85c');
        } catch (e) {
            console.error('error');
        }
        console.log(snowflakeAPI.endpointHistory().insertReport[0].response);
    });

    it('loadHistoryScan', async () => {
        // start time is in ISO 8601 format zulu timezone.  Probably use a library like moment.tz.
        try {
            /* const response =*/ await snowflakeAPI.loadHistoryScan(PIPE_NAME, '2020-09-21T02:00:00.000Z');
        } catch (e) {
            console.error('error', e);
        }
        console.log(snowflakeAPI.endpointHistory().loadHistoryScan[0].response);
    })
});
```

# secrets and environment
You'll want to ensure your secret is in a vault or secret management (I am storing the lookup key as an environment variable). The rest could come from environment or hard coding.  Here is a sample `.env` as above for running locally and against your setup in the cloud likely serverless:
```bash
snowflake_private_key=<your key>
snowflake_account=<snowflake account>
snowflake_username=<user you created with permission to pipe>
snowflake_region_id=<region is optional see docs ie: us-central1>
snowflake_cloud_provider=<optional as well.  see docs: could be gcp (you can get this from your instance website full URL>
```

# adding to your project
You can just copy the one file or add via npm:
```
yarn add snowflake-ingest-node
yarn add jwt-simple
```
There is a peer dependency on `jwt-simple`, so make sure it is added as well.  There are no other dependencies except for built-in node (https and crypto) modules.
