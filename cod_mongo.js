--1.
db.language.aggregate([
    {
        $lookup:{
            from:"film",
            localField:"_id",
            foreignField:"language_id",
            as:"film"
        }
    },
    {
        $unwind:"$film"
    },
    {
        $group:{
            _id:"$name",
            count:{$sum:1}
        }
    }
]
).sort({count:-1})

--2 
db.actor.aggregate([
    {
        $lookup:{
            from:"film_actor",
            localField:"_id",
            foreignField:"actor_id",
            as:"film_actor"
        }
    },
    {
        $unwind:"$film_actor"    
    },
    {
        $group:{
            first_name:"$first_name",
            last_name:"$last_name",
            count:{$sum:1}
        }
    }
])


2. 

db.actor.aggregate([
    {
        $match : {
            "_id": ObjectId("6443f419563ebc7712d126e2")
        }
    },
    {  
        $lookup:{
            from:"film_actor",
            localField:"_id",
            foreignField:"actor_id",
            as:"film_actor"
        }
    },
    {
        $unwind:"$film_actor"    
    },
    {
        $group:{
            _id: {first_name:'$first_name',
            last_name:'$last_name'
        },
            first_name:{$first:'$first_name'},
            last_name:{$first:'$last_name'},
             count:{$sum:1}
        }
    },
    {
        $match: { count: { $gt: 35 } } 
    },
    {
        $project:{
            _id:0,
            first_name:1,
            last_name:1,
            count:1
        }
    }
   
])

//final
db.actor.aggregate([
    {  
        $lookup:{
            from:"film_actor",
            localField:"_id",
            foreignField:"actor_id",
            as:"film_actor"
        }
    },
    {
        $unwind:"$film_actor"    
    },
    {
        $group:{
            _id: {first_name:'$first_name',
            last_name:'$last_name'
        },
            first_name:{$first:'$first_name'},
            last_name:{$first:'$last_name'},
             count:{$sum:1}
        }
    },
    {
        $match: { count: { $gt: 35 } } 
    },
    {
        $project:{
            _id:0,
            first_name:1,
            last_name:1,
            count:1
        }
    }
   
])



