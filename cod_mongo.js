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


// 3.
db.actor.aggregate([
    
    {
        $lookup: {
            from: "film_actor",
            localField: "_id",
            foreignField: "actor_id",
            as: "film_actor"
        }
    },
    {
        $lookup : {
            from: "film",
            localField: "film_actor.film_id",
            foreignField: "_id",
            as: "film"
        }
    },
    {
        $lookup : {
            from: "film_category",
            localField: "film._id",
            foreignField: "film_id",
            as: "film_category"
        }
    },
    {
        $lookup: {
            from: "category",
            localField: "film_category.category_id",
            foreignField: "_id",
            as: "category"
        }
    },
    {
        $match : {
            "category.name": "Comedy"
        }
    },
    {
        $unwind:"$category",    
    },
    {
        $group : {
            _id : "$_id",
            film_count : { $sum : 1 }
        }
    }
])
//--------------------------
db.actor.aggregate([
    {
        $match : {
            "_id": ObjectId("6443f419563ebc7712d126d7")
        }
    },
    
    {
        $lookup: {
            from: "film_actor",
            localField: "_id",
            foreignField: "actor_id",
            as: "film_actor"
        }
    },
    {
        $lookup : {
            from: "film",
            localField: "film_actor.film_id",
            foreignField: "_id",
            as: "film"
        }
    },
    {
        $lookup : {
            from: "film_category",
            localField: "film._id",
            foreignField: "film_id",
            as: "film_category"
        }
    },
    {
        $lookup: {
            from: "category",
            localField: "film_category.category_id",
            foreignField: "_id",
            as: "category"
        }
    },
    {
        $match : {
            "category.name": "Comedy"
        }
    }
]);


db.category.aggregate([
    {
        $match : {
            "name": "Comedy"
        }
    },
    {
        $lookup : {
            from: "film_category",
            localField: "_id",
            foreignField: "category_id",
            as: "film_category"
        }
    }

]);

db.category.aggregate([
    {
        $match : {
            "name": "Comedy"
        }
    },
    {
        $lookup : {
            from: "film_category",
            localField: "_id",
            foreignField: "category_id",
            as: "film_category"
        }
    },
    {
        $lookup : {
            from: "film",
            localField: "film_category.film_id",
            foreignField: "_id",
            as: "film"
        }
    },
    {
        $lookup: {
            from: "film_actor",
            localField: "film._id",
            foreignField: "film_id",
            as: "film_actor"
        }
    },
    {
        $lookup: {
            from: "actor",
            localField: "film_actor.actor_id",
            foreignField: "_id",
            as: "actor"
    }

    },
    {
        $unwind:"$actor"
    },
    {
        $group : {
            _id :"$actor.first_name",
                    
            count : { $sum : 1 }
        }
    },
    {
        $sort : { count : -1 }
    },
    {
        $limit : 10
    }


]);