from sqlalchemy import create_engine

engine = create_engine("sqlite:///sqlalchemy_tuts.db")
engine.connect()

print(engine)

