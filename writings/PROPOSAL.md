# Project Proposal: Analysis of Network Aggregation of YouTube Data 

## Problem 
The entire set of YouTube videos can be conceptualized as a directed graph, where each video “node” is connected to a set of other, recommended video “nodes”. This project seeks to better understand YouTube’s video recommendation algorithm, particularly how a particular video can impact other videos’ views. Variables in this endeavor include category, length, views, and viewer interaction (comments, ratings). With a YouTube dataset, our team aims to determine what combination of variables produce a viral video. 

## Required Tasks 
To complete this project, our team will need to do: 

### Set-up 
- Obtain a suitable system for running Spark and its related computations.
- Convert, clean, and normalize the dataset into a suitable CSV format with Python. 
- Categories should be represented as an ID number in a separate table.
- Load into Spark SQL using Python. 

### Data Processing 
- Produce a suitable video graph network using the Spark GraphX library.
- Represent videos as vertices and related video links as directed edges. 
- Perform degree calculations, focusing on: (a) Incoming/outgoing edges per vertex; min./max./average per type of edges, (b) Degree distribution, as a histogram or cumulative distribution visual, and (c) Categorized statistics: frequency of videos partitioned by a search condition.

### Categorization, size of videos, view count, etc.  
- Utilize Python to create an application for producing human-readable output and examine results
- Write documentation for others to use our project and interpret their findings. 
- Create the final report and project presentation, detailing our entire project including the results and subsequent drawn conclusions.  

## Technologies/Tools/Systems 
Technologies, tools, and systems used for this project include, but are not limited to: 
- Python for dataset Extract-Transform-Load, and user-end application. 
- System for running Apache Spark and associated tools to create a graph. 
- GitHub for easy access, sharing, GitHub Codespaces, and version control.  
- Markdown for writing documentation, describing project objectives 

## Implementation/Evaluation Plan (justification) 
Initially, we will work on familiarizing ourselves with the dataset and begin drafting a plan on how we will work with it. Then, we will begin working with Python to extract, transform, and load the data. Any data cleaning or other formatting will likely take place within Python as well, as it provides libraries that easily work with large groupings of data. 

Time will be taken to determine which big data analytics engine is appropriate for this project (e.g. standalone Spark, Spark with Hadoop).  Data will be transferred into the selected solution and use frameworks to create the directed graph. All code and related files will be stored and shared across GitHub, allowing team collaboration on this project.  

## Timeline and milestones 
The following timeline is broken into the four months that this project will take place over. The class assignment milestones are labeled with their due date, while the rest create a general outline of when general or specific project tasks should be completed. These will aide us in meeting these deadlines. 

### February
- Project Proposal (24th). 
- Start working with the dataset, understanding its contents. 
- Begin to work with and understand the software that we will be working with for this project, namely Apache Spark. 

### March  
- Use Python to produce a normalized set of CSV files. 
- Design database schema to store data appropriately. 
- Begin porting from Python to Apache Spark. 

### April 
- Ensure results are producible and understandable. 
- Work on results examination and compilation into final report and presentation. 
- Project Presentation Submission (15th). 
- Project Presentation (28th). 

### May 
- Any final adjustments or fixes from presentation feedback, finishing touches. 
- Final Report (6th). 