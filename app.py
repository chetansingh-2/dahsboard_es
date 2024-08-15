import os

import pandas as pd
import streamlit as st
from elasticsearch import Elasticsearch
ES_INDEX = 'srilanka_raw_data'
es = Elasticsearch(
    cloud_id=os.getenv("cloud_id"),
    api_key=os.getenv("api_key")

)

# Define Streamlit app
# def main():
#     st.title('Sri Lanka News Data')
#
#     # Retrieve unique values for dropdowns
#     unique_provinces, _ = get_unique_provinces_and_districts()
#
#     # Select province
#     selected_province = st.selectbox('Select Province', unique_provinces)
#     if selected_province:
#         unique_districts = get_districts_by_province(selected_province)
#         selected_district = st.selectbox('Select District (Optional)', unique_districts + ['None'])
#     else:
#         selected_district = None
#
#     # Checkbox to show/hide content
#     show_content = st.checkbox('Show Content', value=False)
#
#     if st.button('Fetch Data'):
#         if selected_province:
#             data = query_elasticsearch(selected_province, selected_district)
#             data = query_elasticsearch(selected_province, selected_district)
#             total_count = len(data)
#             st.write(f"Total feeds: {total_count}")
#             if data:
#                 st.write(f"Showing data for Province: {selected_province} and District: {selected_district}")
#                 df = format_data(data, show_content)
#                 st.dataframe(df, width=1200)  # Adjust the width as needed
#             else:
#                 st.write("No data found.")
#         else:
#             st.write("Please select a province.")


def main():
    st.title('Sri Lanka News Data')

    # Retrieve unique values for dropdowns
    unique_provinces, _ = get_unique_provinces_and_districts()
    unique_provinces = ['All Provinces'] + unique_provinces

    # Select province
    selected_province = st.selectbox('Select Province', unique_provinces)
    if selected_province and selected_province != 'All Provinces':
        unique_districts = get_districts_by_province(selected_province)
        unique_districts = ['All Districts'] + unique_districts
        selected_district = st.selectbox('Select District (Optional)', unique_districts)
    else:
        selected_district = None

    # Checkbox to show/hide content
    show_content = st.checkbox('Show Content', value=False)

    if st.button('Fetch Data'):
        data = query_elasticsearch(selected_province, selected_district)
        total_count = len(data)
        st.write(f"Total feeds: {total_count}")
        if data:
            if selected_province == 'All Provinces':
                st.write(f"Showing data for all provinces")
            elif selected_district == 'All Districts':
                st.write(f"Showing data for Province: {selected_province} and all districts")
            else:
                st.write(f"Showing data for Province: {selected_province} and District: {selected_district}")

            df = format_data(data, show_content)
            st.dataframe(df, width=1200)  # Adjust the width as needed
        else:
            st.write("No data found.")


def get_unique_provinces_and_districts():
    province_query = {
        "size": 0,
        "aggs": {
            "provinces": {
                "nested": {
                    "path": "sri_lanka.province"
                },
                "aggs": {
                    "unique_provinces": {
                        "terms": {
                            "field": "sri_lanka.province.name",
                            "size": 1000
                        }
                    }
                }
            }
        }
    }

    try:
        province_response = es.search(index=ES_INDEX, body=province_query)
        provinces = province_response['aggregations']['provinces']['unique_provinces']['buckets']
        unique_provinces = [bucket['key'] for bucket in provinces]
        return unique_provinces, []
    except KeyError as e:
        st.error(f"KeyError: {e}")
        return [], []
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return [], []

def get_districts_by_province(province):
    district_query = {
        "size": 0,
        "aggs": {
            "provinces": {
                "nested": {
                    "path": "sri_lanka.province"
                },
                "aggs": {
                    "province_filter": {
                        "filter": {
                            "term": {
                                "sri_lanka.province.name": province
                            }
                        },
                        "aggs": {
                            "districts": {
                                "nested": {
                                    "path": "sri_lanka.province.district"
                                },
                                "aggs": {
                                    "unique_districts": {
                                        "terms": {
                                            "field": "sri_lanka.province.district.name",
                                            "size": 1000
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    try:
        district_response = es.search(index=ES_INDEX, body=district_query)
        districts = district_response['aggregations']['provinces']['province_filter']['districts']['unique_districts']['buckets']
        unique_districts = [bucket['key'] for bucket in districts]
        return unique_districts
    except KeyError as e:
        st.error(f"KeyError: {e}")
        return []
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return []
def query_elasticsearch(province, district):
    if province == 'All Provinces':
            query = {
              "query": {
                "match_all": {}
              },
              "size": 5000,
              "sort": [
                {
                  "sri_lanka.province.district.news.datetime": {
                    "order": "desc",
                    "nested": {
                      "path": "sri_lanka.province.district.news"
                    }
                  }
                }
              ]
            }
    elif district and district!='All Districts':
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "nested": {
                                    "path": "sri_lanka.province",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "match": {
                                                        "sri_lanka.province.name": province
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                }
                            },
                            {
                                "nested": {
                                    "path": "sri_lanka.province.district",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "match": {
                                                        "sri_lanka.province.district.name": district
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        ]
                    }
                },
            "size": 5000,
                "sort": [
                    {
                        "sri_lanka.province.district.news.datetime": {
                            "order": "desc",
                            "nested": {
                                "path": "sri_lanka.province.district.news"
                            }
                        }
                    }
                ]

            }
    else:
        query = {
          "query": {
            "nested": {
              "path": "sri_lanka.province",
              "query": {
                "bool": {
                  "must": [
                    {
                      "match": {
                        "sri_lanka.province.name":province
                      }
                    }
                  ]
                }
              }
            }
          },
            "size": 5000,
            "sort": [
                {
                    "sri_lanka.province.district.news.datetime": {
                        "order": "desc",
                        "nested": {
                            "path": "sri_lanka.province.district.news"
                        }
                    }
                }
            ]

        }


    try:
        response = es.search(index=ES_INDEX, body=query)
        return response['hits']['hits']
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return []
def format_data(data, show_content):
    rows = []
    for hit in data:
        source = hit.get('_source', {}).get('sri_lanka', {})
        provinces = source.get('province', [])
        if not isinstance(provinces, list):
            provinces = [provinces]

        for province in provinces:
            districts = province.get('district', [])
            if not isinstance(districts, list):
                districts = [districts]

            for district in districts:
                news_list = district.get('news', [])
                if not isinstance(news_list, list):
                    news_list = [news_list]

                for news in news_list:
                    content = news.get('content', '')
                    if not show_content and content:
                        content = content[:50]
                    media = news.get('media', [])
                    if not isinstance(media, list):
                        if media is None:
                            media = []
                        elif isinstance(media, str):
                            media = [media]
                        else:
                            st.write(f"Unexpected media type: {type(media)} - {media}")
                            media = []

                    rows.append([
                        str(news.get('id', '')[:10]),
                        str(news.get('url', '')),
                        content,
                        news.get('likes', 'N/A'),
                        news.get('views', 'N/A'),
                        news.get('shares', 'N/A'),
                        ', '.join(media),
                        news.get('source', 'N/A'),
                        news.get('datetime', 'N/A')
                    ])

    df = pd.DataFrame(rows, columns=['ID', 'URL', 'Content', 'Likes', 'Views', 'Shares', 'Media', 'Source', 'datetime'])
    return df

if __name__ == '__main__':
    main()