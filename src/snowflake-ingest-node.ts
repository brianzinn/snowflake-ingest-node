import * as crypto from 'crypto';
import { ClientRequest, IncomingMessage } from 'http';
import * as https from 'https';
import * as jwt from 'jwt-simple';

const USER_AGENT = 'snowpipe-ingest-node/0.0.1/node/npm';

export type SnowpipeAPIOptions = {
    recordHistory: boolean
}

export type RecordedCallResponse = {
    error?: Error
    statusCode?: number
    messageBody?: string
}

export type RecordedCall = {
    request: https.RequestOptions
    response: RecordedCallResponse
}

export type APIEndpointHistory = {
    insertFile: RecordedCall[]
    insertReport: RecordedCall[]
    loadHistoryScan: RecordedCall[]
}

/**
 * 
 * @param username user that you created in Snowflake (and added a private key auth)
 * @param privateKey private key for provided user (for generating bearer token)
 * @param account account provided by Snowflake
 * @param regionId needed for non-default (AWS)?
 * @param cloudProvider needed for non-AWS?
 */
export const createSnowpipeAPI = (username: string, privateKey: string, account: string, regionId?: string, cloudProvider?: string, snowpipeAPIOptions?: SnowpipeAPIOptions) => {
    // `{account}.snowflakecomputing.com` is for default US AWS.
    // for GCP you need `{account}.{regionId}.gcp.snowflakecomputing.com`
    const domainParts = [account, regionId, cloudProvider];

    const apiEndpointHistory: APIEndpointHistory = {
        insertFile: [],
        insertReport: [],
        loadHistoryScan: []
    }

    const config = {
        username: username.toUpperCase(),
        privateKey,
        account: account.toUpperCase(),
        hostname: `${domainParts.filter(p => p !== undefined).join('.')}.snowflakecomputing.com`
    }

    const getBearerToken = async (): Promise<string> => {
        const publicKeyBytes = crypto.createPublicKey(privateKey).export({ type: 'spki', format: 'der' });
        // matches FP (fingerprint) on `desc user <username>`in snowflake.
        // ie: SHA256:g....I=
        const signature = 'SHA256:' + crypto.createHash('sha256').update(publicKeyBytes).digest().toString('base64');

        const ISSUER = 'iss';
        const ISSUED_AT_TIME = 'iat';
        const EXPIRY_TIME = 'exp';
        const SUBJECT = 'sub';
        
        const payload = {
            [ISSUER]: `${config.account}.${config.username}.${signature}`,
            [SUBJECT]: `${config.account}.${config.username}`,
            [ISSUED_AT_TIME]: Math.round(new Date().getTime() / 1000),
            [EXPIRY_TIME]: Math.round(new Date().getTime() / 1000 + 60 * 59)
        }

        if (jwt === undefined) {
            console.error('"jwt-simple" not found (make sure to include this peer dependency in your project).')
        }

        const bearer = jwt.encode(payload, privateKey, 'RS256');
        return bearer;
    }
    
    const makeRequest = async (options: https.RequestOptions, endpointCallHistory: RecordedCall[], postBody?: string): Promise<string> => {
        return new Promise<string>((resolve, reject) => {
            const req: ClientRequest = https.request(
                options,
                (response: IncomingMessage) => {
                    const body: string[] = [];
                    response.on('data', (chunk: any) => {
                        body.push(chunk);
                    })

                    response.on('end', () => {
                        const messageBody = body.join('');
                        if (snowpipeAPIOptions?.recordHistory === true) {
                            endpointCallHistory.push({
                                request: options,
                                response: {
                                    statusCode: response.statusCode,
                                    messageBody
                                }
                            });
                        }

                        if (response.statusCode !== undefined && (response.statusCode < 200 || response.statusCode > 299)) {
                            reject(new Error(`status code: ${response.statusCode}.  '${messageBody}'`));
                        } else {
                            resolve(messageBody);
                        }
                    })
                }
            );

            req.on('error', (error: Error) => {
                if (snowpipeAPIOptions?.recordHistory === true) {
                    endpointCallHistory.push({
                        request: options,
                        response: {
                            error
                        }
                    });
                }
                reject(error);
            })
    
            if (postBody) {
                req.write(postBody);
            }
    
            req.end();
        });
    }
    
    /**
     * Snowflake recommends providing a random string with each request, e.g. a UUID.
     */
    const getRequestId = () => {
        return crypto.randomBytes(16).toString("hex");
    }
    
    /**
     * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html#data-file-ingestion
     * 
     * @param filenames list of files to be ingested by snowflake
     * @param pipeName Case-sensitive, fully-qualified pipe name. For example, myDatabase.mySchema.myPipe.
     */
    const insertFile = async (filenames: string[], pipeName: string): Promise<string> => {
        const postBody = JSON.stringify({
            "files": filenames.map(filename => ({path: filename}))
        });
    
        const path = `/v1/data/pipes/${pipeName}/insertFiles?requestId=${getRequestId()}`;
    
        const jwtToken: string = await getBearerToken();
    
        const options: https.RequestOptions = {
            hostname: config.hostname,
            port: 443,
            path,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': postBody.length,
                'Authorization': `Bearer ${jwtToken}`,
                'User-Agent': USER_AGENT,
                Accept: 'application/json'
            }
        };
        return await makeRequest(options, apiEndpointHistory.insertFile, postBody);
    }
    
    /**
     * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html#endpoint-insertreport
     */
    const insertReport = async (pipeName: string, beginMark?: string) => {
        // https://<account>.snowflakecomputing.com/v1/data/pipes/<pipeName>/insertReport?requestId=
        let path = `/v1/data/pipes/${pipeName}/insertReport?requestId=${getRequestId()}`;
        if (beginMark) {
            path += `&beginMark=${beginMark}`;
        }
    
        const jwtToken: string = await getBearerToken();
    
        const options: https.RequestOptions = {
            hostname: config.hostname,
            port: 443,
            path,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${jwtToken}`,
                'User-Agent': USER_AGENT,
                Accept: 'application/json'
            }
        };
        return await makeRequest(options, apiEndpointHistory.insertReport);
    }
    
    /**
     * 
     * @param pipeName Case-sensitive, fully-qualified pipe name. For example, myDatabase.mySchema.myPipe.
     * @param startTimeInclusive Timestamp in ISO-8601 format. Start of the time range to retrieve load history data.
     * @param endTimeExclusive Timestamp in ISO-8601 format. End of the time range to retrieve load history data. If omitted, then CURRENT_TIMESTAMP() is used as the end of the range.
     */
    const loadHistoryScan = async (pipeName: string, startTimeInclusive: string, endTimeExclusive?: string) => {
        // /v1/data/pipes/{pipeName}/loadHistoryScan?startTimeInclusive=<startTime>&endTimeExclusive=<endTime>&requestId=<requestId>
        let path = `/v1/data/pipes/${pipeName}/loadHistoryScan?startTimeInclusive=${startTimeInclusive}&requestId=${getRequestId()}`;
        if (endTimeExclusive) {
            path += `&endTimeExclusive=${endTimeExclusive}`;
        }
    
        const jwtToken: string = await getBearerToken();
    
        const options: https.RequestOptions = {
            hostname: config.hostname,
            port: 443,
            path,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${jwtToken}`,
                'User-Agent': USER_AGENT,
                Accept: 'application/json'
            }
        };
        return await makeRequest(options, apiEndpointHistory.loadHistoryScan);
    }

    const endpointHistory = (): APIEndpointHistory => {
        return apiEndpointHistory
    }

    return {
        loadHistoryScan,
        insertReport,
        insertFile,
        endpointHistory,
    }
}
