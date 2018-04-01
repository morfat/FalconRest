import serpy

class UserListSerializer(serpy.Serializer):
    _id=serpy.IntField()
    first_name=serpy.Field()
    last_name=serpy.Field()
    phone_number=serpy.Field()
    permissions=serpy.Field(call=True)

