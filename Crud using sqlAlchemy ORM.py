from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from DefiningSchemaInSqlAlchemyORM import Customer

engine = create_engine("sqlite:///sqlalchemy_tuts.db")
session = Session(bind=engine)

from sqlalchemy.orm import sessionmaker, Session
Session = sessionmaker(bind=engine)

session = Session()


c1 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com',
              address='1662 Kinney Street',
              town='Wolfden'
              )

c2 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com',
              address='424 Patterson Street',
              town='Beckinsdale'
              )
c1, c2

c1.first_name, c1.last_name
c2.first_name, c2.last_name

session.add(c1)
session.add(c2)

c1.id, c2.id

session.add_all([c1, c2])

session.new