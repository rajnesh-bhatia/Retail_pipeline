
# we can add api key in env variable and get it in script that will be secure
api_key = 'e69bfe98fab3babb4cc759695f1aeb21'  # "ADD YOUR API KEY " 

# adjust your url Accordingly
database_name = 'Retail'
schema = 'retail'
postgres_url = f"jdbc:postgresql://localhost/{database_name}" 

properties = {
    "driver": "org.postgresql.Driver",
    "user" : "postgres",  # ADD YOUR USER FOR POSTGRES
    "password" : "postgres"  # ADD YOUR PASSWORD FOR POSTGRES
}
