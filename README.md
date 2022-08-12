# Metrics Model - Conversion Rate
Based on Metrics Model package in progress - Metrics Model makes metrics combine metrics together, you could find us [here](https://github.com/chaoss/wg-metrics-models). This readme inspired by [Sir Mordred](https://github.com/chaoss/grimoirelab-tutorial/blob/master/docs/getting-started/setup-cfg.md). 

Currently produces a visualization of numerical conversion rate calculated at regular intervals over time, as well as a list of converted individuals (as uuids). 

## conf.yaml (Configuration File)

The `conf.yaml` file controls all the parameters and pre-processing filters for calculating the constituent metrics and also for the metric model algorithm. It is divided into sections that control different parts of the workflow. 

### [url:] 

- **url:** Connection string. Example: "https://user:password@ip:port" (Required)

### [general:]

- **json_file:** json File that contains projects and repositories to analyze (similar to GrimoireLab's `projects.json`) (Required)


...under construction
    


## Conversion Rate Enricher Info
General info about this enricher: 
This enricher is used for calculating the Conversion Rate metric model. It is best used in conjunction with enrichers for other platforms, e.g. other than Github. This is the Github enricher. 

Rich data obtained from raw:
- Github
    - Test
- GithubQL (Events)
- Github2 
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
