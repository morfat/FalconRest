

class User:
    def __init__(self, _id,phone_number,password,first_name,last_name=None):
        self._id=_id
        self.phone_number=phone_number
        self.password=password
        self.first_name=first_name
        self.last_name=last_name

    
    def permissions(self):
        return [1,2,3,4,5]


        
    




        

