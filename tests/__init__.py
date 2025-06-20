# pg = PostgresConnector()

# async def main():
#     await pg.connect()

#     rows = await pg.fetch_all("SELECT id, name FROM users")
#     for r in rows:
#         print(dict(r))

#     await pg.close()

# if __name__ == "__main__":
#     asyncio.run(main())