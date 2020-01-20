import threading

from flask import Flask, render_template, request

from parser import main, get_logs

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        csv_file = request.form["csv_file"]
        dir_path = request.form['dir_path']
        do_not_split = request.form['do_not_split']

        # Example: do_not_split = ['PS/2', 'I/O', 'Wet/Dry']
        do_not_split = [a.strip() for a in do_not_split.split(',')]

        t = threading.Thread(target=main, args=(dir_path, csv_file, do_not_split,))

        t.start()

    return render_template('index.html')


@app.route("/log", methods=["GET"])
def logs():
    return "<br/>".join(get_logs())


if __name__ == "__main__":
    app.run(debug=True)
