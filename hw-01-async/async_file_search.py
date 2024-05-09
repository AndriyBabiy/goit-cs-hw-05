import argparse
import asyncio
import logging
from aiopath import AsyncPath
from aioshutil import copyfile

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def read_folder(path: AsyncPath) -> None:
  async for file in path.iterdir():
    if await file.is_file():
      await copy_file(file)

async def copy_file(file: AsyncPath) -> None:
  extension_name = file.suffix[1:]
  extension_folder = AsyncPath(args.output) / extension_name

  await extension_folder.mkdir(exist_ok=True, parents=True)
  await copyfile(file, extension_folder / file.name)
  logging.info(f'Copied {file} to {extension_folder / file.name}')

async def main(source: AsyncPath, output: AsyncPath) -> None:
  await read_folder(source)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redistribute files based on their file extension to specific folders.")
    parser.add_argument("--source", type=str, required=True, help="Source directory to redistribute files from")
    parser.add_argument("--output", type=str, required=True, help="Output directory to redistribute files into")

    args = parser.parse_args()

    source_path = AsyncPath(args.source)
    output_path = AsyncPath(args.output)

    asyncio.run(main(source_path, output_path))
