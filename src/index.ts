import postgres from 'postgres';

/** Abstraction around underlying PostgreSQL connection library. */
export type Db = postgres.Sql<{}>;

export type DatabaseUrl = string;
export type DbConfig = {
    databaseUrl: DatabaseUrl
};

/** Must be UUID. Use `import { randomUUID } from 'node:crypto';` to generate one. */
export type MessageId = string;
/** Stream name that contains `-` follows the pattern of `<category>-<rest>`, where the `rest` is usually an identifier of the stream. */
export type StreamName = string;
/** Category groups uniquely identified streams of the same type together. */
export type Category = string;
export type EventType = string;
/** Payload for the particular EventType. Can be any JSON object or even null. */
export type Payload = object;
/** Optional metadata assigned to the message. Can be null. */
export type Metadata = object;
/**
 * Represents a generic entity stored in Message DB.
 * 
 * More info can be found here: http://docs.eventide-project.org/user-guide/message-db/server-functions.html#write-a-message
 */
export type Message = {
    id: MessageId,
    stream_name: StreamName,
    type: EventType,
    data: Payload,
    metadata?: Metadata,
    expectedVersion?: number
};
export type Sql = string;

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#acquire-a-lock-for-a-stream-name
 *  
 * @param db 
 * @returns 
 */
export const acquireLock = (db: Db) => async (
    streamName: StreamName
): Promise<number> => {
    type Result = {
        acquire_lock: number
    };
    const [result] = await db<Result[]>`SELECT message_store.acquire_lock(${streamName})`;
    return result.acquire_lock;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-the-cardinal-id-from-a-stream-name
 * 
 * @param db 
 * @returns 
 */
export const getCardinalId = (db: Db) => async (
    streamName: StreamName
): Promise<string | null> => {
    type Result = {
        cardinal_id: string
    };
    const [result] = await db<Result[]>`SELECT message_store.cardinal_id(${streamName})`;
    return result.cardinal_id;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-the-category-from-a-stream-name
 *  
 * @param db 
 * @returns 
 */
export const getCategory = (db: Db) => async (
    streamName: StreamName
): Promise<string> => {
    type Result = {
        category: string
    };
    const [result] = await db<Result[]>`SELECT message_store.category(${streamName})`;
    return result.category;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-messages-from-a-category
 * 
 * @param db 
 * @returns 
 */
export const getCategoryMessages = (db: Db) => async (
    category: Category,
    position: number = 1,
    batchSize: number = 1000,
    correlation: string | null = null,
    consumerGroupMember: number | null = null,
    consumerGroupSize: number | null = null,
    condition: Sql | null = null
): Promise<any> => {
    const result = await db`SELECT id::varchar,
    stream_name::varchar,
    type::varchar,
    position::bigint,
    global_position::bigint,
    data::jsonb,
    metadata::jsonb,
    time::timestamp FROM message_store.get_category_messages(${category}, ${position}, ${batchSize}, ${correlation}, ${consumerGroupMember}, ${consumerGroupSize}, ${condition})`;
    return result;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-last-message-from-a-stream
 * 
 * @param db 
 * @returns 
 */
export const getLastStreamMessage = (db: Db) => async (
    streamName: StreamName,
    type: EventType | null = null
): Promise<any> => {
    const result = await db`SELECT id::varchar,
    stream_name::varchar,
    type::varchar,
    position::bigint,
    global_position::bigint,
    data::jsonb,
    metadata::jsonb,
    time::timestamp FROM message_store.get_last_stream_message(${streamName}, ${type})`;
    return result;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-messages-from-a-stream
 *  
 * @param db 
 * @returns 
 */
export const getStreamMessages = (db: Db) => async (
    streamName: StreamName,
    position: number = 0,
    batchSize: number = 1000,
    condition: Sql | null = null
): Promise<any> => {
    const result = await db`SELECT id::varchar,
    stream_name::varchar,
    type::varchar,
    position::bigint,
    global_position::bigint,
    data::jsonb,
    metadata::jsonb,
    time::timestamp FROM message_store.get_stream_messages(${streamName}, ${position}, ${batchSize}, ${condition})`;
    return result;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#calculate-a-64-bit-hash-for-a-stream-name
 *  
 * @param db 
 * @returns 
 */
export const getHash64 = (db: Db) => async (
    value: string
): Promise<number> => {
    type Result = {
        hash_64: number
    };
    const [result] = await db<Result[]>`SELECT message_store.hash_64(${value})`;
    return result.hash_64;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-the-id-from-a-stream-name
 *  
 * @param db 
 * @returns 
 */
export const getId = (db: Db) => async (
    streamName: StreamName
): Promise<string | null> => {
    type Result = {
        id: string | null
    };
    const [result] = await db<Result[]>`SELECT message_store.id(${streamName})`;
    return result.id;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-message-store-database-schema-version 
 * 
 * @param db 
 * @returns 
 */
export const getMessageStoreVersion = (db: Db) => async (): Promise<string> => {
    type Result = {
        message_store_version: string
    };
    const [result] = await db<Result[]>`SELECT message_store.message_store_version()`;
    return result.message_store_version;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-stream-version-from-a-stream
 *  
 * @param db 
 * @returns 
 */
export const getStreamVersion = (db: Db) => async (
    streamName: StreamName
): Promise<number> => {
    type Result = {
        stream_version: number
    };
    const [result] = await db<Result[]>`SELECT message_store.stream_version(${streamName})`;
    return result.stream_version;
};

/**
 * http://docs.eventide-project.org/user-guide/message-db/server-functions.html#write-a-message
 *  
 * @param db 
 * @returns 
 */
export const writeMessage = (db: Db) => async (
    message: Message
): Promise<number> => {
    type Result = {
        write_message: number
    };
    const [result] = await db<Result[]>`SELECT message_store.write_message(${message.id}, ${message.stream_name}, ${message.type}, ${message.data as any || null}, ${message.metadata as any || null}, ${message.expectedVersion || null})`;
    return result.write_message;
};

export const create = (config: DbConfig) => {
    // console.debug(`starting ...`, config);
    const db: postgres.Sql<{}> = postgres(config.databaseUrl);
    // console.debug(`connected ...`, config);

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

