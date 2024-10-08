import os
import pandas as pd
import streamlit as st
from elasticsearch import Elasticsearch

# es = Elasticsearch(
#     cloud_id=os.getenv("cloud_id"),
#     api_key=os.getenv("api_key")
# )

es = Elasticsearch(
    cloud_id="social_data:Y2VudHJhbGluZGlhLmF6dXJlLmVsYXN0aWMtY2xvdWQuY29tOjQ0MyRjMjFiZjk3YTE0ZTY0ZDlkOTc0MDJmZjJmNTY3YmIyYiQ1Mjc0MjY4MmY2MTM0NDdjYTE3MjBmZGZkNDI5ZDJmMQ==",
    api_key="TndRRjZKQUJ1bms0VS1NZkJKNjc6WFhQTjhPMmJTSG1RTDc0dWh6ZThWUQ=="
)

def main():
    st.title('News Feeds of India and Sri Lanka')
    tab1, tab2 = st.tabs(["Sri Lanka News Feeds", "India News Feeds"])
    with tab1:
        st.header("Sri Lanka News Feeds")
        handle_sri_lanka_feed()

    with tab2:
        st.header("India News Feeds")
        handle_india_mapping()

def handle_sri_lanka_feed():
    # Your existing Sri Lanka feed logic here
    unique_provinces, _ = get_unique_provinces_and_districts()
    unique_provinces = ['All Provinces'] + unique_provinces
    selected_province = st.selectbox('Select Province', unique_provinces)
    if selected_province and selected_province != 'All Provinces':
        unique_districts = get_districts_by_province(selected_province)
        unique_districts = ['All Districts'] + unique_districts
        selected_district = st.selectbox('Select District (Optional)', unique_districts)
    else:
        selected_district = None

    show_content = st.checkbox('Show content', value=False)
    warning_text = """
        <div style='position: fixed; bottom: 10px; width: 50%; background-color: #f8d7da; padding: 10px; text-align: left; color: #721c24; font-size: 14px; border-top: 1px solid #f5c6cb;'>
            ⚠️ Note: Avoid querying large samples when not required (e.g. fetching entire state data). 
        </div>
        """
    st.markdown(warning_text, unsafe_allow_html=True)
    if st.button('Fetch Sri Lanka Data'):
        data = query_elasticsearch_srilanka(selected_province, selected_district)
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
            st.dataframe(df, width=1800)
        else:
            st.write("No data found.")


def handle_india_mapping():
    # State selection
    states = ['haryana', 'jammu and kashmir', 'jharkhand']
    selected_state = st.selectbox('Select State', states)

    mapping_indices = {
        'haryana': 'constituency_mapping_haryana',
        'jammu and kashmir': 'constituency_mapping_jammu_and_kashmir',
        'jharkhand': 'constituency_mapping_jharkhand'
    }

    state_news_indices = {
        'haryana': 'haryana_raw_data',
        'jammu and kashmir': 'jammu_and_kashmir_raw_data',
        'jharkhand': 'jharkhand_raw_data',
    }

    selected_index = mapping_indices.get(selected_state)
    unique_districts = get_districts_by_state(selected_index)

    # Add "All Districts" option
    unique_districts.insert(0, "All Districts")

    selected_district = st.selectbox('Select District (Optional)', unique_districts)
    selected_news_index = state_news_indices.get(selected_state)
    show_content = st.checkbox('Show Content', value=False)
    warning_text = """
           <div style='position: fixed; bottom: 10px; width: 50%; background-color: #f8d7da; padding: 10px; text-align: left; color: #721c24; font-size: 14px; border-top: 1px solid #f5c6cb;'>
               ⚠️ Note: Avoid querying large samples when not required (e.g. fetching entire state data). 
           </div>
           """
    st.markdown(warning_text, unsafe_allow_html=True)
    if st.button('Fetch India Data'):
        # Adjust the query based on whether "All Districts" is selected
        if selected_district == "All Districts":
            data,total_count= query_elasticsearch_india(selected_news_index, selected_district)
        else:
            data, _ = query_elasticsearch_india(selected_news_index, selected_district)

            total_count = len(data)
        st.write(f"Total feeds for selected params: {total_count}")
        if data:
            st.write(f"Showing data for State: {selected_state}, District: {selected_district}")
            df = format_india_data(data, show_content)
            st.dataframe(df, width=1800)
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
        province_response = es.search(index="srilanka_raw_data", body=province_query)
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
        district_response = es.search(index="srilanka_raw_data", body=district_query)
        districts = district_response['aggregations']['provinces']['province_filter']['districts']['unique_districts']['buckets']
        unique_districts = [bucket['key'] for bucket in districts]
        return unique_districts
    except KeyError as e:
        st.error(f"KeyError: {e}")
        return []
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return []

def query_elasticsearch_srilanka(province, district):
    if province == 'All Provinces':
            query = {
              "query": {
                "match_all": {}
              },
              "size": 10000,
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
            "size": 10000,
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
            "size": 10000,
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
        response = es.search(index="srilanka_raw_data", body=query)
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

def get_districts_by_state(index):
    query = {
        "size": 0,
        "aggs": {
            "unique_districts": {
                "terms": {
                    "field": "district.keyword",
                    "size": 1000
                }
            }
        }
    }
    try:
        response = es.search(index=index, body=query)
        districts = response['aggregations']['unique_districts']['buckets']
        return [bucket['key'] for bucket in districts]
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return []


def query_elasticsearch_india(index, district):
    if district == "All Districts":
        # Get the total count of documents
        try:
            count_query = {
                "query": {
                    "match_all": {}
                }
            }
            total_count_response = es.count(index=index, body=count_query)
            total_count = total_count_response['count']
        except Exception as e:
            st.error(f"An error occurred while getting the count: {e}")
            total_count = 0

        # Get only the first 10,000 documents
        query = {
            "query": {
                "match_all": {}
            },
            "size": 10000,
            "sort": [
                {
                    "news.datetime": {
                        "order": "desc",
                        "nested": {
                            "path": "news"
                        }
                    }
                }
            ]
        }
    else:
        total_count = None
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "district": district
                            }
                        }
                    ]
                }
            },
            "size": 10000,
            "sort": [
                {
                    "news.datetime": {
                        "order": "desc",
                        "nested": {
                            "path": "news"
                        }
                    }
                }
            ]
        }

    try:
        response = es.search(index=index, body=query)
        documents = response['hits']['hits']
    except Exception as e:
        st.error(f"An error occurred while searching: {e}")
        documents = []
        total_count = 0

    return documents, total_count

def format_india_data(data, show_content):
    rows = []

    for hit in data:
        source = hit.get('_source', {})
        news_list = source.get('news', [])
        if not isinstance(news_list, list):
            news_list = [news_list]
        for news in news_list:
            content = news.get('content', '')
            if not show_content and content:
                content = content[:50]
            media = news.get('media', [])
            if media is None:
                media = []
            elif not isinstance(media, list):
                media = [media]
            # Ensure all elements in media are strings and handle None values
            media = [str(m) if m is not None else '' for m in media]
            rows.append([
                str(news.get('id', '')[:10]),
                str(news.get('url', '')),
                content,
                news.get('likes', 'N/A'),
                news.get('views', 'N/A'),
                news.get('shares', 'N/A'),
                ', '.join(media),
                news.get('source', 'N/A'),
                news.get('datetime', 'N/A'),
            ])

    # Create DataFrame
    df = pd.DataFrame(rows, columns=['ID', 'URL', 'Content', 'Likes', 'Views', 'Shares', 'Media', 'Source', 'Datetime'])
    return df
if __name__ == '__main__':
    main()
