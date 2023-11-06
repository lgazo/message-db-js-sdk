import postgres from 'postgres';

export type Db = postgres.Sql<{}>;

export type DatabaseUrl = string;
export type DbConfig = {
    databaseUrl: DatabaseUrl
};

// Function that wraps around the acquire_lock stored procedure
export const acquireLock = (db: Db) => async (
    streamName: string
): Promise<number> => {
    type Result = {
        acquire_lock: number
    };
    const [ result ] = await db<Result[]>`SELECT message_store.acquire_lock(${streamName})`;
    return result.acquire_lock;
};

// Function that wraps around the cardinal_id stored procedure
export const getCardinalId = (db: Db) => async (
    streamName: string
): Promise<string | null> => {
    type Result = {
        cardinal_id: string
    };
    const [ result ] = await db<Result[]>`SELECT message_store.cardinal_id(${streamName})`;
    return result.cardinal_id;
};

// Function that wraps around the category stored procedure
export const getCategory = (db: Db) => async (
    streamName: string
): Promise<string> => {
    type Result = {
        category: string
    };
    const [ result ] = await db<Result[]>`SELECT message_store.category(${streamName})`;
    return result.category;
};

// Function that wraps around the get_category_messages stored procedure
export const getCategoryMessages = (db: Db) => async (
    category: string,
    position: number = 1,
    batchSize: number = 1000,
    correlation: string | null = null,
    consumerGroupMember: number | null = null,
    consumerGroupSize: number | null = null,
    condition: string | null = null
): Promise<any> => {
    const result = await db`SELECT * FROM message_store.get_category_messages(${category}, ${position}, ${batchSize}, ${correlation}, ${consumerGroupMember}, ${consumerGroupSize}, ${condition})`;
    return result;
};

// Function that wraps around the get_last_stream_message stored procedure
export const getLastStreamMessage = (db: Db) => async (
    streamName: string,
    type: string | null = null
): Promise<any> => {
    const result = await db`SELECT * FROM message_store.get_last_stream_message(${streamName}, ${type})`;
    return result;
};

// Function that wraps around the get_stream_messages stored procedure
export const getStreamMessages = (db: Db) => async (
    streamName: string,
    position: number = 0,
    batchSize: number = 1000,
    condition: string | null = null
): Promise<any> => {
    const result = await db`SELECT * FROM message_store.get_stream_messages(${streamName}, ${position}, ${batchSize}, ${condition})`;
    return result;
};

// Function that wraps around the hash_64 stored procedure
export const getHash64 = (db: Db) => async (
    value: string
): Promise<number> => {
    type Result = {
        hash_64: number
    };
    const [ result ] = await db<Result[]>`SELECT message_store.hash_64(${value})`;
    return result.hash_64;
};

// Function that wraps around the id stored procedure
export const getId = (db: Db) => async (
    streamName: string
): Promise<string | null> => {
    type Result = {
        id: string | null
    };
    const [ result ] = await db<Result[]>`SELECT message_store.id(${streamName})`;
    return result.id;
};

// Function that wraps around the message_store_version stored procedure
export const getMessageStoreVersion = (db: Db) => async (): Promise<string> => {
    type Result = {
        message_store_version:string
    };
    const [ result ] = await db<Result[]>`SELECT message_store.message_store_version()`;
    return result.message_store_version;
};

// Function that wraps around the stream_version stored procedure
export const getStreamVersion = (db: Db) => async (
    streamName: string
): Promise<number> => {
    type Result = {
        stream_version: number
    };
    const [ result ] = await db<Result[]>`SELECT message_store.stream_version(${streamName})`;
    return result.stream_version;
};

export const writeMessage = (db: Db) => async (
    id: string,
    streamName: string,
    type: string,
    data: object,
    metadata: object | null = null,
    expectedVersion: number | null = null
): Promise<number> => {
    type Result = {
        write_message: number
    };
    const [ result ] = await db<Result[]>`SELECT message_store.write_message(${id}, ${streamName}, ${type}, ${JSON.stringify(data)}, ${JSON.stringify(metadata)}, ${expectedVersion})`;
    return result.write_message;
};

export const create = (config: DbConfig) => {
    console.debug(`starting ...`, config);
    const db: postgres.Sql<{}> = postgres(config.databaseUrl, {
        connection: {
            search_path: 'message_store,public'
        }
    });
    console.debug(`connected ...`, config);

    return {
        acquireLock: acquireLock(db),
        getCardinalId: getCardinalId(db),
        getCategory: getCategory(db),
        getCategoryMessages: getCategoryMessages(db),
        getLastStreamMessage: getLastStreamMessage(db),
        getStreamMessages: getStreamMessages(db),
        getHash64: getHash64(db),
        getId: getId(db),
        getMessageStoreVersion: getMessageStoreVersion(db),
        getStreamVersion: getStreamVersion(db),
        writeMessage: writeMessage(db)
    }
};

