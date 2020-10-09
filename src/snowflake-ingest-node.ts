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
 * Have seen the response return a 403 when things were misconfigured in HTML response.
 */
export type InsertFileResponse = {
    /**
     * This is the one we send.  (Could try with an empty requestId in request to see if one is assigned)
     */
    requestId: string,
    /**
     * When response code is "SUCCESS" it was received.
     * Haven't seen other codes and not documented on snowpipe API page.
     */
    responseCode: 'SUCCESS'
}

/**
 * A success response (200) contains information about files that have recently been added to the table. Note that this report may only represent a portion of a large file.
 */
export type InsertReportResponse = {
    /**
     * The fully-qualified name of the pipe.
     */
  pipe: string
  /**
   * false if an event was missed between the supplied beginMark and the first event in this report history. Otherwise, true.
   */
  completeResult: boolean
  /**
   * beginMark to use on the next request to avoid seeing duplicate records. Note that this value is a hint. Duplicates can still occasionally occur.
   */
  nextBeginMark: string
  /**
   * An array of JSON objects, one object for each file that is part of the history response.
   */
  files: SnowpipeFile[]
}

export enum LoadStatus {
    /**
     * The entire file has been loaded into the table.
     */
    LOADED= 'LOADED',

    /**
     * Part of the file has been loaded into the table, but the load process has not completed yet.
     */
    LOAD_IN_PROGRESS='LOAD_IN_PROGRESS',

    /**
     * The file load failed.
     */
    LOAD_FAILED= 'LOAD_FAILED',

    /**
     * Some rows from this file were loaded successfully, but others were not loaded due to errors. Processing of this file is completed.
     */
    PARTIALLY_LOADED= 'PARTIALLY_LOADED',
}

export type SnowpipeFile = {
    /**
     * The file path relative to the stage location.
     */
    path: string,
    /**
     * Either the stage ID (internal stage) or the S3 bucket (external stage) defined in the pipe.
     */
    stageLocation: string,
    /**
     * File size, in bytes.
     */
    fileSize: number,
    /**
     * Time that this file was received for processing. Format is ISO-8601 in UTC time zone.
     */
    timeReceived: string,
    /**
     * Time that data from this file was last inserted into the table. Format is ISO-8601 in UTC time zone.
     */
    lastInsertTime: string,
    /**
     * Number of rows inserted into the target table from the file.
     */
    rowsInserted: number,
    /**
     * Number of rows parsed from the file. Rows with errors may be skipped.
     */
    rowsParsed: number,
    /**
     * Number of errors seen in the file
     */
    errorsSeen: number,
    /**
     * Number of errors allowed in the file before it is considered failed (based on ON_ERROR copy option).
     */
    errorLimit: number,
    /**
     * Error message for the first error encountered in this file.
     */
    firstError?: string,
    /**
     * Line number of the first error.
     */
    firstErrorLineNum?: number,
    /**
     * Column name where the first error occurred.
     */
    firstErrorColumnName?: string,
    /**
     * General error describing why the file was not processed.
     */
    systemError?: string,
    /**
     * Indicates whether the file was completely processed successfully.
     */
    complete: boolean,
    /**
     * Load status for the file:
     */
    status: LoadStatus
}

/**
 * A success response (200) contains information about files that have recently been added to the table. Note that this report may only represent a portion of a large file.
 */
export type LoadHistoryScanResponse = {
    /**
     * An array of JSON objects, one object for each file that is part of the history response.
     */
    files: SnowpipeFile[],
    /**
     * Starting timestamp (in ISO-8601 format) provided in the request.
     */
    startTimeInclusive: string,
    /**
     * Ending timestamp (in ISO-8601 format) provided in the request.
     */
    endTimeExclusive: string,
    /**
     * Timestamp (in ISO-8601 format) of the oldest entry in the files included in the response.
     */
    rangeStartTime: string,
    /**
     * Timestamp (in ISO-8601 format) of the latest entry in the files included in the response.
     */
    rangeEndTime: string,
    /**
     * Fully-qualified name of the pipe.
     */
    pipe: string,
    /**
     * false if the report is incomplete (i.e. the number of entries in the specified time range exceeds the 10,000 entry limit).
     * If false, the user can specify the current rangeEndTime value as the startTimeInclusive value for the next request to proceed to the next set of entries.
     */
    completeResult: "true" | "false"
}

export type SnowpipeAPIResponse<T> = {
    json: T | null
    rawResponse: string
    statusCode?: number
}

export interface SnowpipeAPI {
    /**
     * Fetches a report about ingested files whose contents have been added to table. Note that for large files, this may only be part of the file.
     * This endpoint differs from insertReport in that it views the history between two points in time. There is a maximum of 10,000 items returned, but multiple calls can be issued to cover the desired time range.
     * 
     * @param pipeName Case-sensitive, fully-qualified pipe name. For example, myDatabase.mySchema.myPipe.
     * @param startTimeInclusive Timestamp in ISO-8601 format. Start of the time range to retrieve load history data.
     * @param endTimeExclusive Timestamp in ISO-8601 format. End of the time range to retrieve load history data. If omitted, then CURRENT_TIMESTAMP() is used as the end of the range.
     */
    loadHistoryScan: (pipeName: string, startTimeInclusive: string, endTimeExclusive?: string) => Promise<SnowpipeAPIResponse<LoadHistoryScanResponse>>;
    /**
     * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html#endpoint-insertreport
     * 
     * @param pipeName Case-sensitive, fully-qualified pipe name. For example, myDatabase.mySchema.myPipe.
     * @param beginMark optional (see docs)
     */
    insertReport: (pipeName: string, beginMark?: string) => Promise<SnowpipeAPIResponse<InsertReportResponse>>;
    /**
     * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html#data-file-ingestion
     * 
     * @param filenames list of files to be ingested by snowflake
     * @param pipeName Case-sensitive, fully-qualified pipe name. For example, myDatabase.mySchema.myPipe.
     */
    insertFile: (pipeName: string, filenames: string[], postJSON?: boolean) => Promise<SnowpipeAPIResponse<InsertFileResponse>>;
    /**
     * full call history with response/response pairs.
     */
    readonly endpointHistory: APIEndpointHistory;
}

/**
 * 
 * @param username user that you created in Snowflake (and added a private key auth)
 * @param privateKey private key for provided user (for generating bearer token)
 * @param account account provided by Snowflake
 * @param regionId needed for non-default (AWS)?
 * @param cloudProvider needed for non-AWS?
 */
export const createSnowpipeAPI = (username: string, privateKey: string, account: string, regionId?: string, cloudProvider?: string, snowpipeAPIOptions?: SnowpipeAPIOptions): SnowpipeAPI => {
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

    const makeRequest = async <T>(options: https.RequestOptions, endpointCallHistory: RecordedCall[], postBody?: string): Promise<SnowpipeAPIResponse<T>> => {
        return new Promise<SnowpipeAPIResponse<T>>((resolve, reject) => {
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
                            let json: T | null = null;
                            try {
                                json = JSON.parse(messageBody) as T;
                            } catch (e) {
                                console.warn(`unable to parse response (expecting valid JSON on a ${response.statusCode} status code`, messageBody);
                            }
                            resolve({
                                json,
                                rawResponse: messageBody,
                                statusCode: response.statusCode
                            } as SnowpipeAPIResponse<T>);
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

    const insertFile = async (pipeName: string, filenames: string[], postJSON: boolean = false): Promise<SnowpipeAPIResponse<InsertFileResponse>> => {
        let contentType;
        let postBody;
        if (postJSON === true) {
            contentType = 'application/json';
            postBody = JSON.stringify({
                "files": filenames.map(filename => ({ path: filename }))
            });
        } else {
            contentType = 'text/plain';
            postBody = filenames.join('\n');
        }

        const path = `/v1/data/pipes/${pipeName}/insertFiles?requestId=${getRequestId()}`;

        const jwtToken: string = await getBearerToken();

        const options: https.RequestOptions = {
            hostname: config.hostname,
            port: 443,
            path,
            method: 'POST',
            headers: {
                'Content-Type': contentType,
                'Content-Length': postBody.length,
                'Authorization': `Bearer ${jwtToken}`,
                'User-Agent': USER_AGENT,
                Accept: 'application/json'
            }
        };
        return await makeRequest<InsertFileResponse>(options, apiEndpointHistory.insertFile, postBody);
    }

    const insertReport = async (pipeName: string, beginMark?: string): Promise<SnowpipeAPIResponse<InsertReportResponse>> => {
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
        return await makeRequest<InsertReportResponse>(options, apiEndpointHistory.insertReport);
    }

    const loadHistoryScan = async (pipeName: string, startTimeInclusive: string, endTimeExclusive?: string): Promise<SnowpipeAPIResponse<LoadHistoryScanResponse>> => {
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
        return await makeRequest<LoadHistoryScanResponse>(options, apiEndpointHistory.loadHistoryScan);
    }

    return {
        loadHistoryScan,
        insertReport,
        insertFile,
        endpointHistory: apiEndpointHistory,
    }
}
