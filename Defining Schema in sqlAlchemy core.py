from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, Text, DateTime, Boolean, ForeignKey

metadata = MetaData()

user = Table('users', metadata,
    Column('id', Integer(), primary_key=True),
    Column('user', String(200), nullable=False),
)

posts = Table('posts', metadata,
    Column('id', Integer(), primary_key=True),
    Column('post_title', String(200), nullable=False),
    Column('post_slug', String(200),  nullable=False),
    Column('content', Text(),  nullable=False),
    Column('user_id', Integer(), ForeignKey("users.id")),
)

for t in metadata.tables:
    print(metadata.tables[t])

print('-------------')

for t in metadata.sorted_tables:
    print(t.name)

print(posts.columns)         # return a list of columns
print(posts.c)               # same as posts.columns
print(posts.foreign_keys)    # returns a set containing foreign keys on the table
print(posts.primary_key)     # returns the primary key of the column
print(posts.metadata)        # get the MetaData object from the table
print(posts.columns.post_title.name)     # returns the name of the column
print(posts.columns.post_title.type)     # returns the type of the column
