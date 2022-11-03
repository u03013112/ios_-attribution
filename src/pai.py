# pai demo
import tensorflow as tf

print('tensorflow in ~')

tables = ["odps://rivergamepai_v2/tables/tmp_cv_d1p7_202206_202208_double"]
print(tables)
dataset = tf.data.TableRecordDataset(tables,record_defaults = (0,0.0),selected_cols = "id,cv0")
print(dataset)

# Get a batch of 128
dataset = dataset.batch(128)
# Set epoch as 10
dataset = dataset.repeat(10)
# At last we got a batch of ids and prices.
[ids, cv0s] = dataset.make_one_shot_iterator().get_next()
print(ids,cv0s)
