# Avito ad parser

Parsing Avito ads according to specified criteria. In the above example, there is one category "apartments for sale" and a list of the largest cities and administrative centers of the regions. The data is collected in a common json file, and a separate file is created for each city

### Before starting the programm
- log in to the avito website in browser
- copy headers values from developer tools and fill appropriate rows in main.py (cookie - row #49, user_agent - row #50, accept - row #51, if_none_match - row #52, accept_encoding - row #53)
- open category link which you want parse (for example, "https://www.avito.ru/tula/kvartiry/prodam-ASgBAgICAUSSA8YQ"). And get name of category from url (for example, "kvartiry") and param value from tag <link> with attribute rel=alternate (for example, in tag for android or ios "[201]=1059")
![image](https://user-images.githubusercontent.com/108010119/206897194-c181e00a-980e-4805-a22a-397805ace467.png)
- set parameters of ad for parsing in variables "firebase" and "params". For apartments I seted basic parameters. If you want to get ad in other categories, you can start programm once and choose needed parameters from file progress.txt (values "List firebase" and "List params")
- start main.py
- In the end we will get json files with ads and report progress.txt
