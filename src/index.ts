import pgPromise from "pg-promise";
import type { IClient } from "pg-promise/typescript/pg-subset";

// Initialize pg-promise and connect to your database
const pgp = pgPromise();

export type Db = pgPromise.IDatabase<{}, IClient>;
export type DatabaseUrl = string;
export type DbConfig = {
    databaseUrl: DatabaseUrl
};

// Function that wraps around the acquire_lock stored procedure
export const acquireLock = (db: Db) => async (
    streamName: string
): Promise<number> => {
    return db.one(
        `SELECT message_store.acquire_lock($1)`,
        [streamName],
        (a: { acquire_lock: number }) => a.acquire_lock
    );
};

// Function that wraps around the cardinal_id stored procedure
export const getCardinalId = (db: Db) => async (
    streamName: string
): Promise<string | null> => {
    return db.oneOrNone(
        `SELECT message_store.cardinal_id($1)`,
        [streamName],
        (a: { cardinal_id: string | null }) => a.cardinal_id
    );
};

// Function that wraps around the category stored procedure
export const getCategory = (db: Db) => async (
    streamName: string
): Promise<string> => {
    return db.one(
        `SELECT message_store.category($1)`,
        [streamName],
        (a: { category: string }) => a.category
    );
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
    return db.any(
        `SELECT * FROM message_store.get_category_messages($1, $2, $3, $4, $5, $6, $7)`,
        [category, position, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition]
    );
};

// Function that wraps around the get_last_stream_message stored procedure
export const getLastStreamMessage = (db: Db) => async (
    streamName: string,
    type: string | null = null
): Promise<any> => {
    return db.any(
        `SELECT * FROM message_store.get_last_stream_message($1, $2)`,
        [streamName, type]
    );
};

// Function that wraps around the get_stream_messages stored procedure
export const getStreamMessages = (db: Db) => async (
    streamName: string,
    position: number = 0,
    batchSize: number = 1000,
    condition: string | null = null
): Promise<any> => {
    return db.any(
        `SELECT * FROM message_store.get_stream_messages($1, $2, $3, $4)`,
        [streamName, position, batchSize, condition]
    );
};

// Function that wraps around the hash_64 stored procedure
export const getHash64 = (db: Db) => async (
    value: string
): Promise<number> => {
    return db.one(
        `SELECT message_store.hash_64($1)`,
        [value],
        (a: { hash_64: number }) => a.hash_64
    );
};

// Function that wraps around the id stored procedure
export const getId = (db: Db) => async (
    streamName: string
): Promise<string | null> => {
    return db.oneOrNone(
        `SELECT message_store.id($1)`,
        [streamName],
        (a: { id: string | null }) => a.id
    );
};

// Function that wraps around the message_store_version stored procedure
export const getMessageStoreVersion = (db: Db) => async (): Promise<string> => {
    return db.one(
        `SELECT message_store.message_store_version()`,
        [],
        (a: { message_store_version: string }) => a.message_store_version
    );
};

// Function that wraps around the stream_version stored procedure
export const getStreamVersion = (db: Db) => async (
    streamName: string
): Promise<number> => {
    return db.one(
        `SELECT message_store.stream_version($1)`,
        [streamName],
        (a: { stream_version: number }) => a.stream_version
    );
};

export const writeMessage = (db: Db) => async (
    id: string,
    streamName: string,
    type: string,
    data: object,
    metadata: object | null = null,
    expectedVersion: number | null = null
): Promise<number> => {
    return db.one(
        `SELECT message_store.write_message($1, $2, $3, $4, $5, $6)`,
        [id, streamName, type, data, metadata, expectedVersion],
        (a: { write_message: number }) => a.write_message
    );
};

export const create = (config: DbConfig) => {
    console.debug(`starting ...`, config);
    // const db = pgp(DATABASE_URL);
    const db = pgp(config.databaseUrl);
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
        getStreamVersion: getStreamMessages(db),
        writeMessage: writeMessage(db)
    }
};

