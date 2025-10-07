# PE Ownership Checker (Streamlit)

A tiny Streamlit app that:
1) looks up a company on Wikipedia,
2) detects if it’s PE-owned/backed using simple heuristics, and
3) suggests peer companies that don’t appear PE-owned.

## Deploy on Streamlit Community Cloud
1. Create a new GitHub repo and add **app.py** and **requirements.txt** (this folder’s files).
2. Go to https://streamlit.io/cloud and click **New app**.
3. Select your repo, branch, and set **Main file path** to `app.py`.
4. Deploy. That’s it.

_No secrets required; it only calls the public Wikipedia API._
