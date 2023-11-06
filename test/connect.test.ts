import { describe, it, expect } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Message, create } from '../src';

const DATABASE_URL = process.env.DATABASE_URL || '';

const assertDatabaseUrl = () => {
    if (!DATABASE_URL) {
        throw new Error('Create `.env` file with `DATABASE_URL` PG connection string');
    }
}
describe('should', () => {
    it('get message store version', async () => {
        assertDatabaseUrl();

        console.info(`do something`);
        const { getMessageStoreVersion } = create({ databaseUrl: DATABASE_URL });
        const result = await getMessageStoreVersion();
        console.info(`done`, result);

        expect(result).toBe('1.3.0');
    });

    it('write and get message', async () => {
        assertDatabaseUrl();

        const { 
            getStreamMessages,
            writeMessage
        } = create({ databaseUrl: DATABASE_URL });

        const timestamp = randomUUID();
        const stream_name = `messageDbJsSdk__test-${timestamp}`;
        const message: Message = {
            id: randomUUID(),
            stream_name: stream_name,
            type: 'ConsumedTest',
            data: {
                timestamp
            }
        }
        await writeMessage(message);
        const result = await getStreamMessages(stream_name);
        // console.info(`done`, result);

        expect(result.length).toBe(1);
        const one: Message = result[0];
        expect(one.id).toBe(message.id);
        expect(one.stream_name).toBe(message.stream_name);
        expect(one.type).toBe(message.type);
        expect((one.data as Record<string, unknown>).timestamp).toBe((message.data as Record<string,unknown>).timestamp);
        expect(one.metadata).toBeNull();
    });


});