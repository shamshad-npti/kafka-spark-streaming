"""
SalesReceipt and ItemOrder model classes
"""
import json

def _assign_with_type_check(obj, attr, value, type_info):
    if isinstance(value, type_info):
        setattr(obj, attr, value)
    else:
        raise TypeError("Expected type: %s but found: %s" % (type_info, type(value)))

class SalesReceipt(object):
    def __init__(self, store_id, customer_id, receipt_id, items):
        self._store_id = int(store_id)
        self._customer_id = int(customer_id)
        self._receipt_id = int(receipt_id)

        if not isinstance(items, (list, tuple)):
            raise TypeError("items should be instance of list or tuple")

        if not all([isinstance(item, ItemOrder) for item in items]):
            raise TypeError("All item order should be instance of `ItemOrder`")

        self._items = tuple(items)

    @property
    def store_id(self):
        return self._store_id

    @store_id.setter
    def store_id(self, value):
        _assign_with_type_check(self, '_store_id', value, int)

    @property
    def customer_id(self):
        return self._customer_id

    @customer_id.setter
    def customer_id(self, value):
        _assign_with_type_check(self, '_customer_id', value, int)

    @property
    def receipt_id(self):
        return self._receipt_id

    @receipt_id.setter
    def receipt_id(self, value):
        _assign_with_type_check(self, '_receipt_id', value, int)

    @property
    def items(self):
        return self._items

    def as_dict(self):
        return {
            "store_id": self.store_id,
            "customer_id": self.customer_id,
            "receipt_id": self.receipt_id,
            "items": [item.as_dict() for item in self.items]
        }

    def __str__(self):
        return json.dumps(self.as_dict())


class ItemOrder(object):
    def __init__(self, item_id, quantity, total_price):
        self._item_id = int(item_id)
        self._quantity = int(quantity)
        self._total_price = float(total_price)

    @property
    def item_id(self):
        return self._item_id

    @item_id.setter
    def item_id(self, value):
        _assign_with_type_check(self, '_item_id', value, int)

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        _assign_with_type_check(self, '_quantity', value, int)

    @property
    def total_price(self):
        return self._total_price

    @total_price.setter
    def total_price(self, value):
        _assign_with_type_check(self, '_total_price', value, float)

    def as_dict(self):
        return {
            "item_id": self.item_id,
            "quantity": self.quantity,
            "total_price_paid": self.total_price
        }

    def __str__(self):
        return json.dumps(self.as_dict())
