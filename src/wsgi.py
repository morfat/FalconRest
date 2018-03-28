
#This contains the deployable content for the whole project

import falcon


from urls import URLS as V1_URLS 

def add_routes(app,*urls):
    for u in urls:
        for app_url in u[1]:
            for url in app_url[1]:
                route_path=u[0]+app_url[0]+url[0]
                route_resource=url[1]
                #register urls
                app.add_route(route_path,route_resource() )
  
    
def get_app():

    app=falcon.API(media_type='application/json')
 
    add_routes(app,('/v1',V1_URLS),) #list of routes per views 

    return app


application=get_app()