#!/usr/bin/env python
from api.error import Error
from frameworks.bottle import Bottle, response, run, static_file, route
from controller.home import HomeController
from controller.static import StaticController
from os.path import join, dirname

# Setup App
app = Bottle()
appPath = dirname(__file__)

#Setup APIs below



#Setup Controllers
home_controller = HomeController()
static_controller = StaticController()


#Handle errors
@app.error(code=400)
@app.error(code=401)
@app.error(code=403)
@app.error(code=404)
@app.error(code=405)
@app.error(code=406)
@app.error(code=409)
@app.error(code=500)
@app.error(code=500)
@app.error(code=501)
@app.error(code=502)
def handle_error(error):
    return Error.handle_error(response, error)

def main():
    # Static Routes - /static/styles.css
    app.route("/static/<filename>", method="GET", callback=static_controller.file)

    # API Routes
    
    
    # Web routes
    app.route("/", method="GET", callback=home_controller.index)
   
   # In order to change port to 80, you must first run as sudo
   # host = bind address. 0.0.0.0 for all adresses on port, or URL
   # Debug gives some info
    app.run(host='0.0.0.0', port=8080, debug=True)



if __name__ == "main" or __name__ == "__main__":
    main()


