from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://root:pass@localhost/mydb")

engine.connect()

print(engine)
