import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

# Snowflake credentials (from secrets.toml or environment variables)
sf_options = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
}

# Initialize the Snowflake session
session = Session.builder.configs(sf_options).create()

# Fetch book details from Snowflake
# Fetch book details from Snowflake based on the book title
def fetch_book_details(book_title):
    query = f"""
    SELECT ISBN, BOOK_TITLE, BOOK_AUTHOR, YEAR_OF_PUBLICATION, PUBLISHER, 
           IMAGE_URL_S, IMAGE_URL_M, IMAGE_URL_L, BOOK_RATING
    FROM TEST_DB.PUBLIC.BOOKS
    WHERE BOOK_TITLE ILIKE '{book_title}'
    """
    df = session.sql(query).to_pandas()
    return df

# Optimized Recommendation Query (No Popular Book Fallback) based on ISBN
def fetch_recommendations(isbn):
    query = f"""
    WITH UserInteractions AS (
        -- Get details of users who have read the input book
        SELECT DISTINCT USER_ID, LOCATION, AGE
        FROM TEST_DB.PUBLIC.BOOKS 
        WHERE ISBN = '{isbn}'
    ),
    SimilarUsers AS (
        -- Identify users in the same location and similar age range
        SELECT DISTINCT b.USER_ID
        FROM TEST_DB.PUBLIC.BOOKS b
        JOIN UserInteractions u ON 
            b.LOCATION = u.LOCATION 
            AND ABS(b.AGE - u.AGE) <= 5
        WHERE b.ISBN <> '{isbn}'
    ),
    BooksReadBySimilarUsers AS (
        -- Books read by similar users, ranked by user count & rating
        SELECT 
            b.ISBN, 
            b.BOOK_TITLE, 
            b.BOOK_AUTHOR, 
            b.PUBLISHER, 
            b.IMAGE_URL_S, 
            AVG(b.BOOK_RATING) AS AVG_RATING, 
            COUNT(DISTINCT b.USER_ID) AS USER_COUNT
        FROM TEST_DB.PUBLIC.BOOKS b
        JOIN SimilarUsers su ON b.USER_ID = su.USER_ID
        WHERE b.ISBN <> '{isbn}'
        GROUP BY b.ISBN, b.BOOK_TITLE, b.BOOK_AUTHOR, b.PUBLISHER, b.IMAGE_URL_S
        ORDER BY USER_COUNT DESC, AVG_RATING DESC
        LIMIT 5
    )
    SELECT * FROM BooksReadBySimilarUsers;
    """
    
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"❌ Error while fetching recommendations: {e}")
        return pd.DataFrame()


# Streamlit App
def main():
    st.set_page_config(page_title="Book Recommendation System", layout="wide")
    st.title("📚 Book Recommendation System")
    st.write("Enter a Book Title to get details and top 5 personalized book recommendations!")

    book_title = st.text_input("Enter Book Title:")

    if st.button("🔍 Get Recommendations"):
        # Fetch book details based on the entered book title
        book_df = fetch_book_details(book_title)

        if not book_df.empty:
            book = book_df.iloc[0]

            col1, col2 = st.columns([1, 2])
            with col1:
                st.image(book["IMAGE_URL_M"], width=150)
            with col2:
                st.subheader(f"**{book['BOOK_TITLE']}**")
                st.write(f"📖 **Author:** {book['BOOK_AUTHOR']}")
                st.write(f"🏢 **Publisher:** {book['PUBLISHER']}")
                st.write(f"⭐ **Rating:** {book['BOOK_RATING']}")

            st.divider()

            # Use the ISBN from the fetched book details to get recommendations
            recommendations_df = fetch_recommendations(book["ISBN"])
            if not recommendations_df.empty:
                st.subheader("🔝 Top 5 Recommended Books")

                for _, rec in recommendations_df.iterrows():
                    with st.expander(f"📘 {rec['BOOK_TITLE']} by {rec['BOOK_AUTHOR']}"):
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.image(rec["IMAGE_URL_S"], width=120)
                        with col2:
                            st.write(f"🏢 **Publisher:** {rec['PUBLISHER']}")
                            st.write(f"⭐ **Average Rating:** {rec['AVG_RATING']:.2f}")
            else:
                st.warning("⚠️ No personalized recommendations found. Try another book.")
        else:
            st.error("❌ Book not found. Please enter a valid book title.")

if __name__ == "__main__":
    main()
