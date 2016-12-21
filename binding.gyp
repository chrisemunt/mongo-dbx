{
  "targets": [
    {
      "target_name": "mongox",
      "defines": [
                    "MONGO_STATIC_BUILD",
                    "MONGO_HAVE_STDINT"
                 ],
      "include_dirs": [
                         "src/mongo"
                      ],
      "sources": [
                    "src/mongox.cpp",
                    "src/mongo/bcon.c",
                    "src/mongo/bson.c",
                    "src/mongo/env.c",
                    "src/mongo/encoding.c",
                    "src/mongo/gridfs.c",
                    "src/mongo/md5.c",
                    "src/mongo/mongo.c",
                    "src/mongo/numbers.c"
                 ]
    }
  ]
}
