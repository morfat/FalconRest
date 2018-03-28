import falcon

class UserListCreateView:
   
    def on_post(self,req,resp):
        resp.media={}

    
    def on_get(self,req,resp):
        resp.media=[]


