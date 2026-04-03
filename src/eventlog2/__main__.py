from .app import app


def main() -> None:
    app.run(debug=True, port=8080)


if __name__ == "__main__":
    main()
