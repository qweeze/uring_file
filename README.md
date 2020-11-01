### Requires

- python 3.8+
- linux kernel 5.7+

### Usage

```python
f = uring_file.File('hello.txt')
await f.open(os.O_CREAT | os.O_WRONLY)
await f.write(b'hello\nworld')
await f.close()

# or as a context manager:
async with uring_file.open('hello.txt') as f:
    async for line in f:
        print(line)
```
