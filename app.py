import streamlit as st
import pandas as pd

# existing logic
from logic import process_dataframe

st.set_page_config(page_title="CSV Cleaner", layout="centered")

st.title("CSV Cleansing Tool")

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file, encoding="utf-8")

    st.success("File uploaded successfully")
    st.write("Preview:")
    st.dataframe(df.head())

    if st.button("Process CSV"):
        result_df = process_dataframe(df)

        csv = result_df.to_csv(index=False).encode("utf-8-sig")

        st.download_button(
            label="Download cleaned CSV",
            data=csv,
            file_name="cleaned_output.csv",
            mime="text/csv"
        )
