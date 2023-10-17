from msedge.selenium_tools import Edge, EdgeOptions

# Create Microsoft Edge options
edge_options = EdgeOptions()
# Add any desired options here, e.g., to run Edge in headless mode:
# edge_options.add_argument("headless")

# Create an Edge WebDriver instance
driver = Edge(options=edge_options)

# Open a website
website_url = "https://www.realestate.com.au/sold/in-brisbane+-+greater+region,+qld/list-1"  # Replace with the URL you want to open
driver.get(website_url)

page_source = driver.page_source
