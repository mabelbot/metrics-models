## Metrics Model - Conversion Rate
Metrics Model makes metrics combine  metrics together, you could find us [here](https://github.com/chaoss/wg-metrics-models) 

### please create conf.yaml file as following way:

    url:
        "https://user:password@ip:port"  
    params: 
        {
        'issue_index': Issue index  
        'json_file': json file for repos messages
        'git_index':  git index 
        'git_branch': None,
        'from_date': the beginning of time for metric model
        'end_date': the end of time for metric model
        'out_index': new index for metric model
        'community': the name of community
        'level': representation of the metrics, choose from repo, project, community.
        }

params is designed to init Metric Model. 


# Element
General info about this enricher: 
This enricher is used for calculating the Conversion Rate metric model. It is best used in conjunction with enrichers for other platforms, e.g. other than Github. This is the Github enricher. 

Rich data obtained from raw:
- Processes the following information about issues: 
    - Issue author, issue author date/time.
    - Issue events, their Github login and also date of event occurrence obtained through the timeline
        - List of valid actions: https://docs.github.com/en/developers/webhooks-and-events/events/issue-event-types
        - This API endpoint will be useful: https://api.github.com/repos/chaoss/augur/issues/49/timeline
    - Reactions on the original issue, Github login of reactors and date of event
- Processes the following information about pull requests:
- Processes the following information about repositories * (what is the purpose of repo enriching?)


TODO
- Integrate the timeline occurrence into the raw data fetching instead of workaround