import { describe, it, expect } from 'bun:test';
import { create } from '../src';

const DATABASE_URL = process.env.DATABASE_URL;

describe('should', () => {
    it('get message store version', async () => {
        if(!DATABASE_URL) {
            throw new Error('Create `.env` file with `DATABASE_URL` PG connection string');
        }

        console.info(`do something`);
        const { getMessageStoreVersion } = create({ databaseUrl: DATABASE_URL });
        const result = await getMessageStoreVersion();
        console.info(`done`, result);

        expect(result).toBe('1.3.0');
    });
});