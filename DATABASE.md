# Database Design document
## Dataset Description
The dataset consists of n files named 0.txt, 1.txt, ..., n-1.txt, representing the videos found at each depth level. More specifically, a file contains:

|   Name				|	Type	|	Notes                       	|
|-----------------------|-----------|-----------------------------------|
|   video ID			|   string  |   unique, 11-digits long          |
|   uploader			|   string  |   Video uploader's username       |
|   age					|   int	 	|   # of days since Feb 15, 2007    |
|   category			|   string  |   Video category                  |
|   length				|   int	 	|   Video length                    |
|   views				|   int	 	|   View count                      |
|   rate				|   float   |   Video rate                      |
|   ratings			  	|   int	 	|   # of ratings                    |
|   comments			|	int	  	|   # of comments                   |
|   related videos 1-20 |   string  |   IDs of related videos, up to 20	|

## Intended SQL Schema Design
For step 1 of this project, we must convert the textfiles to .csv files, which can be easily loaded into SQL servers
### Video (Node)
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	id      	|	char(11)	|	!null, unique		|	PK		|
|	uploader	|	int			|	Up.uploader_id		|	FK		|
|	age			|	int			|	unsigned			|			|
|	category	|	int			|	Cat.category_id		|	FK		|
|	length		|	int			|	unsigned			|			|
|	views		|	int			|	unsigned			|			|
|	rate		|	dec(3,2)    |	0.00<->9.99			|			|
|	ratings		|	int			|	unsigned			|			|
|	comments	|	int			|	unsigned			|			|

### Category
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	id	        |	int			|	!null, autoinc.		| 	PK		|
|	category	|	tinytext	|	!null, unique		|			|

### User
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	id	        |	int			|	!null, autoinc.		|	PK		|
|	username	|	tinytext	|	!null, unique		|			|

### Relation (Node-Node connection)
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	video_id	|	char(11)	|	Video.video_id		|	PK,FK	|
|	related_id	|	char(11)	|	Video.video_id		|	PK,FK	|