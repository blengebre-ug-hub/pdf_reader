from bs4 import BeautifulSoup


with open("page.html", "r", encoding="utf-8") as f:
    html = f.read()


soup = BeautifulSoup(html, "html.parser")


for tag in soup.find_all(True):

    for attr, value in tag.attrs.items():

        text = str(value)

        if (
            "pdf" in text.lower()
            or "upload" in text.lower()
        ):

            print("\nTAG:", tag.name)

            print(
                attr,
                "=",
                value
            )

            print("----------------")
