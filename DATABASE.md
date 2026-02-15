# Database Design document
## Dataset Description
The dataset consists of n files named 0.txt, 1.txt, ..., n-1.txt, representing the videos found at each depth level. More specifically, a file contains:

|   Name				|	Type	|	Notes	|
|-----------------------|-----------|-----------|
|   video ID			|   string  |   unique, 11-digits long |
|   uploader			|   string  |   Video uploader's username  |
|   age					|   int	 	|   # of days between video upload date and Feb 15, 2007 (YouTube's establishment) |
|   category			|   string  |   Video category |
|   length				|   int	 	|   Video length   |
|   views				|   int	 	|   View count |
|   rate				|   float   |   Video rate |
|   ratings			  	|   int	 	|   # of ratings   |
|   comments			|	int	  	|   # of comments  |
|   related videos 1-20 |   string  |   IDs of related videos, up to 20	|

## Intended SQL Schema Design
For step 1 of this project, we must convert the textfiles to .csv files, which can be easily loaded into SQL servers
### Video
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	video_id	|	char		|	!null, unique		|	PK		|
|	uploader	|	int			|	Up.uploader_id		|	FK		|
|	age			|	int			|	unsigned			|			|
|	category	|	int			|	Cat.category_id		|	FK		|
|	length		|	int			|	unsigned			|			|
|	views		|	int			|	unsigned			|			|
|	rate		|	decimal		|	0.00<->9.99			|			|
|	ratings		|	int			|	unsigned			|			|
|	comments	|	int			|	unsigned			|			|

### Category
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	category_id	|	int			|	!null, autoinc.		| 	PK		|
|	name		|	tinytext	|	!null, unique		|			|

### Uploader
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	uploader_id	|	int			|	!null, autoinc.		|	PK		|
|	name		|	tinytext	|	!null, unique		|			|

### Relation
|   Name		|   Type		|   Notes 				|	Key		|
|---------------|---------------|-----------------------|-----------|
|	video_id	|	char(11)	|	Video.video_id		|	PK,FK	|
|	related_id	|	char(11)	|	Video.video_id		|	PK,FK	|