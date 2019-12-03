#from IPython.core.interactiveshell import InteractiveShell
#InteractiveShell.ast_node_interactivity = "all"


from sqlalchemy import create_engine, MetaData, Table, Integer, String,Column, DateTime, ForeignKey, Numeric, CheckConstraint

from datetime import datetime

metadata = MetaData()

engine = create_engine("sqlite:///sqlalchemy_tuts.db")

customers = Table('customers', metadata,
    Column('id', Integer(), primary_key=True),
    Column('first_name', String(100), nullable=False),
    Column('last_name', String(100), nullable=False),
    Column('username', String(50), nullable=False),
    Column('email', String(200), nullable=False),
    Column('address', String(200), nullable=False),
    Column('town', String(50), nullable=False),
    Column('created_on', DateTime(), default=datetime.now),
    Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
)


items = Table('items', metadata,
    Column('id', Integer(), primary_key=True),
    Column('name', String(200), nullable=False),
    Column('cost_price', Numeric(10, 2), nullable=False),
    Column('selling_price', Numeric(10, 2),  nullable=False),
    Column('quantity', Integer(), nullable=False),
    CheckConstraint('quantity > 0', name='quantity_check')
)


orders = Table('orders', metadata,
    Column('id', Integer(), primary_key=True),
    Column('customer_id', ForeignKey('customers.id')),
    Column('date_placed', DateTime(), default=datetime.now),
    Column('date_shipped', DateTime())
)


order_lines = Table('order_lines', metadata,
    Column('id', Integer(), primary_key=True),
    Column('order_id', ForeignKey('orders.id')),
    Column('item_id', ForeignKey('items.id')),
    Column('quantity', Integer())
)


metadata.create_all(engine)

ins = customers.insert().values(
    first_name = 'John',
    last_name = 'Green',
    username = 'johngreen',
    email = 'johngreen@mail.com',
    address = '164 Hidden Valley Road',
    town = 'Norfolk'
)

print(str(ins))

print(ins.compile().params)

conn = engine.connect()
print(conn)
r = conn.execute(ins)
print(r)


r.inserted_primary_key
print(type(r.inserted_primary_key))

# metadata.drop_all(engine)

from sqlalchemy import insert

ins = insert(customers).values(
    first_name = 'Katherine',
    last_name = 'Wilson',
    username = 'katwilson',
    email = 'katwilson@gmail.com',
    address = '4685 West Side Avenue',
    town = 'Peterbrugh'
)

r = conn.execute(ins)
print(r.inserted_primary_key)

ins = insert(customers)

r = conn.execute(ins,
    first_name = "Tim",
    last_name = "Snyder",
    username = "timsnyder",
    email = "timsnyder@mail.com",
    address = '1611 Sundown Lane',
    town = 'Langdale'
)
print(r.inserted_primary_key)

r = conn.execute(ins, [
        {
            "first_name": "John",
            "last_name": "Lara",
            "username": "johnlara",
            "email":"johnlara@mail.com",
            "address": "3073 Derek Drive",
            "town": "Norfolk"
        },
        {
            "first_name": "Sarah",
            "last_name": "Tomlin",
            "username": "sarahtomlin",
            "email":"sarahtomlin@mail.com",
            "address": "3572 Poplar Avenue",
            "town": "Norfolk"
        },
        {
            "first_name": "Pablo",
            "last_name": "Gibson",
            "username": "pablogibson",
            "email":"pablogibson@mail.com",
            "address": "3494 Murry Street",
            "town": "Peterbrugh"
        },
        {
            "first_name": "Pablo",
            "last_name": "Lewis",
            "username": "pablolewis",
            "email":"pablolewis@mail.com",
            "address": "3282 Jerry Toth Drive",
            "town": "Peterbrugh"
        },
    ])

print(r.rowcount)


items_list = [
    {
        "name":"Chair",
        "cost_price": 9.21,
        "selling_price": 10.81,
        "quantity": 10
    },
    {
        "name":"Pen",
        "cost_price": 3.45,
        "selling_price": 4.51,
        "quantity": 3
    },
    {
        "name":"Headphone",
        "cost_price": 15.52,
        "selling_price": 16.81,
        "quantity": 50
    },
    {
        "name":"Travel Bag",
        "cost_price": 20.1,
        "selling_price": 24.21,
        "quantity": 50
    },
    {
        "name":"Keyboard",
        "cost_price": 20.12,
        "selling_price": 22.11,
        "quantity": 50
    },
    {
        "name":"Monitor",
        "cost_price": 200.14,
        "selling_price": 212.89,
        "quantity": 50
    },
    {
        "name":"Watch",
        "cost_price": 100.58,
        "selling_price": 104.41,
        "quantity": 50
    },
    {
        "name":"Water Bottle",
        "cost_price": 20.89,
        "selling_price": 25.00,
        "quantity": 50
    },
]

order_list = [
    {
        "customer_id": 1
    },
    {
        "customer_id": 1
    }
]

order_line_list = [
    {
        "order_id": 1,
        "item_id": 1,
        "quantity": 5
    },
    {
        "order_id": 1,
        "item_id": 2,
        "quantity": 2
    },
    {
        "order_id": 1,
        "item_id": 3,
        "quantity": 1
    },
    {
        "order_id": 2,
        "item_id": 1,
        "quantity": 5
    },
    {
        "order_id": 2,
        "item_id": 2,
        "quantity": 4
    },
]

r = conn.execute(insert(items), items_list)
print(r.rowcount)
r = conn.execute(insert(orders), order_list)
print(r.rowcount)
r = conn.execute(insert(order_lines), order_line_list)
print(r.rowcount)


s = customers.select()
print(str(s))


from sqlalchemy import select
s = select([customers])
print(str(s))

r = conn.execute(s)
print(r.fetchall())


rs = conn.execute(s)
for row in rs:
    print(row)

s = select([customers])


r = conn.execute(s)
print(r.fetchone())
print(r.fetchone())
r = conn.execute(s)
r.fetchmany(3)
r.fetchmany(5)


r = conn.execute(s)
print(r.first())

r = conn.execute(s)
print(r.rowcount)

print(r.keys())

r.scalar()

r = conn.execute(s)
row = r.fetchone()
row
type(row)
row['id'], row['first_name']
row[0], row[1]
row[customers.c.id], row[customers.c.first_name]
print(row.id, row.first_name)


s = select([items]).where(
    items.c.cost_price > 20
)

str(s)
r = conn.execute(s)
r.fetchall()

s = select([items]).\
    where(items.c.cost_price + items.c.selling_price > 50).\
    where(items.c.quantity > 10)
print(s)


s = select([items]).\
where(
    (items.c.cost_price > 200 ) |
    (items.c.quantity < 5)
)
print(s)
conn.execute(s).fetchall()

s = select([items]).\
where(
    ~(items.c.quantity == 50)
)
print(s)
conn.execute(s).fetchall()

s = select([items]).\
where(
    ~(items.c.quantity == 50) &
    (items.c.cost_price < 20)
)
print(s)
conn.execute(s).fetchall()

#Using conjunctions
from sqlalchemy import and_, or_, not_


s = select([items]).\
where(
    and_(
        items.c.quantity >= 50,
        items.c.cost_price < 100,
    )
)
print(s)
conn.execute(s).fetchall()


s = select([items]).\
where(
    or_(
        items.c.quantity >= 50,
        items.c.cost_price < 100,
    )
)
print(s)
conn.execute(s).fetchall()

s = select([items]).\
where(
    and_(
        items.c.quantity >= 50,
        items.c.cost_price < 100,
        not_(
            items.c.name == 'Headphone'
        ),
    )
)
print(s)
conn.execute(s).fetchall()

#Is Null operation
s = select([orders]).where(
    orders.c.date_shipped == None
)
print(s)
conn.execute(s).fetchall()
