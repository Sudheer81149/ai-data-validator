import streamlit as st
import json
from validator import basic_validation
from llm_service import analyze_json

# Page configuration
st.set_page_config(
    page_title="JSON Validator",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stTabs [data-baseweb="tab-list"] button {
        font-size: 16px;
    }
    </style>
""", unsafe_allow_html=True)

# Header
st.title("🔍 JSON Validation System")
st.markdown("Validate your JSON data using rule-based and AI-powered validation")

# Sidebar
st.sidebar.header("📋 Options")
input_method = st.sidebar.radio(
    "Choose input method:",
    ["📝 Paste JSON", "📁 Upload File"],
    help="Select how you want to provide JSON data"
)

st.sidebar.markdown("---")
st.sidebar.markdown("### About")
st.sidebar.markdown("""
This tool validates JSON data using:
- **Rule-Based Validation**: Checks basic structure and types
- **LLM Validation**: Uses Google Generative AI for intelligent analysis
""")

# Main content
st.markdown("---")

# Input section
json_data = None
error_message = None

if input_method == "📝 Paste JSON":
    st.subheader("📝 Paste Your JSON")
    json_text = st.text_area(
        "Enter your JSON data:",
        height=200,
        placeholder='{\n  "name": "John",\n  "age": 30\n}'
    )
    
    if json_text:
        try:
            json_data = json.loads(json_text)
            st.success("✅ Valid JSON format!")
        except json.JSONDecodeError as e:
            error_message = f"❌ Invalid JSON: {str(e)}"
            st.error(error_message)

else:  # Upload file
    st.subheader("📁 Upload JSON File")
    uploaded_file = st.file_uploader("Choose a JSON file", type=["json"])
    
    if uploaded_file:
        try:
            json_data = json.load(uploaded_file)
            st.success("✅ File loaded successfully!")
        except json.JSONDecodeError as e:
            error_message = f"❌ Invalid JSON file: {str(e)}"
            st.error(error_message)

# Validation section
if json_data:
    st.markdown("---")
    st.subheader("🚀 Running Validation")
    
    # Add a button to run validation
    if st.button("🔍 Validate JSON", type="primary", use_container_width=True):
        with st.spinner("Validating... This may take a moment..."):
            try:
                # Run rule-based validation
                rule_errors = basic_validation(json_data)
                
                # Run LLM validation
                llm_output = analyze_json(json_data)
                
                # Display results
                st.markdown("---")
                st.subheader("✅ Validation Results")
                
                # Create tabs for results
                tab1, tab2, tab3 = st.tabs(["📊 Summary", "🔧 Rule-Based", "🤖 AI Analysis"])
                
                with tab1:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Rule-Based Errors", len(rule_errors) if isinstance(rule_errors, list) else 0)
                    with col2:
                        st.metric("AI Analysis", "Complete ✅")
                
                with tab2:
                    st.subheader("Rule-Based Validation Results")
                    if isinstance(rule_errors, list):
                        if rule_errors:
                            for i, error in enumerate(rule_errors, 1):
                                st.warning(f"⚠️ Error {i}: {error}")
                        else:
                            st.success("✅ No rule-based errors found!")
                    else:
                        st.info(f"📋 {rule_errors}")
                
                with tab3:
                    st.subheader("AI-Powered Analysis")
                    st.info(llm_output)
                
                # Download results
                st.markdown("---")
                results = {
                    "rule_errors": rule_errors,
                    "llm_output": llm_output,
                    "json_data": json_data
                }
                
                results_json = json.dumps(results, indent=2)
                st.download_button(
                    label="📥 Download Results as JSON",
                    data=results_json,
                    file_name="validation_results.json",
                    mime="application/json"
                )
                
            except Exception as e:
                st.error(f"❌ An error occurred during validation: {str(e)}")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray; font-size: 12px;'>
    Made with ❤️ using Streamlit | Powered by Google Generative AI
</div>
""", unsafe_allow_html=True)
