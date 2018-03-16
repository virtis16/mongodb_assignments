from flask import Flask

app = Flask(__name__)


@app.route("/")
def hello():
    return "Welcome to Python Flask App!"


if __name__ == "__main__":
    app.run()