from contentful_management import Client
from contentful import client


c= client.Client('key1', 'key2')


entries = c.entries()

#for entry in entries:
#    print(entry)

#4zb2ypkbgf8DMc0fQxz2Ty


products_by_price = c.entries({'content_type': 'partner'})

#for entry in products_by_price:
#  print(entry.name)


entry_attributes = {
    'content_type_id': 'partner',
    'fields': {
        'Name': {
            'nb-NO': 'prod'
        },
        'Link': {
            'nb-NO': 'The best product that money cannot buy.'
        },
    }
}


new_entry_id = None
new_entry = c.assets('key1','partner').create(
    new_entry_id,
    entry_attributes)
