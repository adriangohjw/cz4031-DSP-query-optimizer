import streamlit as st
import database
import qep_generator
from compare_logic_new import *

def get_query_result(query):
    # DBConnection takes 5 arguments
    connection = database.DBConnection()
    result = connection.execute(query)[0][0]
    connection.close()
    return result

def get_description(json_obj):
    descriptions = get_qep_text(json_obj)
    # descriptions = to_text(json_obj)
    result = ""
    for description in descriptions:
        result = result + description + "\n"
    return result

def get_tree(json_obj):
    head = extract_qep_data(json_obj)
    # head = parse_json(json_obj)
    return generate_tree("", head)

def get_difference(json_object_A, json_object_B):
    diff = generate_qep_difference(json_object_A, json_object_B)
    # diff = get_diff(json_object_A, json_object_B)
    return diff

@st.cache  # 👈 This function will be cached
def fetch_qep_from_backend(query):
    qep_object = qep_generator.generateQEPs(input_query1)

    alternative_qeps = qep_object.get_alternate_qeps()
    optimal_qep_dbms = qep_object.get_dbms_qep()

    if optimal_qep_dbms in alternative_qeps:
        alternative_qeps.remove(optimal_qep_dbms)

    # Do something really slow in here!

    return alternative_qeps, optimal_qep_dbms

st.title('PostgreSQL Query Optimizer : Fake Picasso')


input_query1 = st.text_area('Enter your query 1 here.', height=400)

# alternative_qeps = None
# optimal_qep_dbms = None

# Convert input query to explain analyse
# if st.button('Submit'):
with st.spinner('Hang on while we optimize your query to go brrr....'):
    alternative_qeps, optimal_qep_dbms = fetch_qep_from_backend(input_query1)

    st.success("Alright! We are ready to speed up your query! ")

    st.write("For the query you have given us, we have been able to determine {} alternative plans as well as the most optimal one as\
            chosen by your DBMS".format(len(alternative_qeps)))

for i in range(len(alternative_qeps)-2):
    print(alternative_qeps[i] == alternative_qeps[i+1])

idx = st.number_input('Select the index of the QEP you want to compare your most optimal plan with', 0, len(alternative_qeps) - 1, 1)
if st.button('Analyse QEPs'):

    query_result_base = optimal_qep_dbms
    query_result_modified = alternative_qeps[idx]

    print(idx)

    query_result_base_obj = json.loads(json.dumps(query_result_base[0][0]))
    query_result_modified_obj = json.loads(json.dumps(query_result_modified[0][0]))

    query_describe_base = get_description(query_result_base_obj)
    query_describe_modified = get_description(query_result_modified_obj)

    col1, col2 = st.beta_columns(2)

    with col1:
        st.write(query_describe_base)
    with col2:
        st.write(query_describe_modified)

    # query_result_base_tree = get_tree(query_result_base_obj)
    # query_result_modified_tree = get_tree(query_result_modified_obj)
    query_result_diff = get_difference(query_result_base_obj, query_result_modified_obj)

    st.write('Query Difference: \n', query_result_diff)
#
#

