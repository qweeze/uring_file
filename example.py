import asyncio
import os

import uring_file


async def test_read_write():
    f = uring_file.File('hello.txt')
    await f.open(os.O_CREAT | os.O_WRONLY)
    await f.write(b'hello\nworld')
    await f.close()

    async with uring_file.open('hello.txt') as f:
        async for line in f:
            print(line)


if __name__ == '__main__':
    asyncio.run(test_read_write())
