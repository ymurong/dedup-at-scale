def mapreduce(input_partitions, f_map, f_reduce, num_reducers=2, print_debug_text=False):
    if print_debug_text: print("---Starting MAP-phase.---")
    # These arrays will hold the outputs of the map-phase for each input partition
    map_output_partitions = []
    # We are running the map-phase on each input partition now. In a real mapreduce system, this would run
    # in parallel on different machines.
    for counter, input_partition in enumerate(input_partitions):
        if print_debug_text: print(f"  Applying f_map to input partition {counter}")
        map_output = []
        # We apply f_map to each (key, value) pair in the input partition, and store the corresponding outputs
        for key, value in input_partition:
            if print_debug_text: print(f"    f_map({key}, {value}) -> ")
            for mapped_key, mapped_value in f_map(key, value):
                if print_debug_text: print(f"      ({mapped_key}, {mapped_value})")
                map_output.append((mapped_key, mapped_value))
        # Store output partition of this map operation
        map_output_partitions.append(map_output)

        # Next, we start the shuffle phase. We need to create several reducer partitions from the map outputs,
    # assign each key to a partition and collect all the values for this key.
    if print_debug_text: print("\n---Starting SHUFFLE-phase.---")
    reduce_input_partitions = []
    # We create as many reducer partitions as specified by num_reducers
    for _ in range(0, num_reducers):
        reduce_input_partitions.append(dict())

    # We shuffle each map output partition now. In a real mapreduce system, this would run
    # in parallel on different machines.
    for counter, map_output_partition in enumerate(map_output_partitions):
        if print_debug_text: print(f"  Shuffling map output input partition {counter}")
        # We process each (key, value) pair from the map-output here
        for key, value in map_output_partition:
            # We determine the target partition for this key via hash-partitioning
            target_partition_index = abs(hash(key)) % num_reducers
            if print_debug_text: print(
                f"    Assigning key [{key}] to reducer input {target_partition_index} via hash-partitioning")
            # We add the value to the group of the key in the target partition
            target_partition = reduce_input_partitions[target_partition_index]
            if not key in target_partition:
                target_partition[key] = []
            target_partition[key].append(value)

    # Next, we run the reduce-phase
    if print_debug_text: print("\n---Starting REDUCE-phase.---")
    reduce_output_partitions = []

    # We reduce each reduce partition now. In a real mapreduce system, this would run
    # in parallel on different machines.
    for counter, reduce_input_partition in enumerate(reduce_input_partitions):
        reduce_output = []
        if print_debug_text: print(f"  Applying f_reduce to reduce_input partition {counter}")
        # We apply f_reduce to each key and its corresponding values now and collect the results
        for key, values in reduce_input_partition.items():
            if print_debug_text: print(f"    f_reduce({key}, {values}) -> ")
            for reduced_key, reduced_value in f_reduce(key, values):
                if print_debug_text: print(f"      ({reduced_key}, {reduced_value})")
                reduce_output.append((reduced_key, reduced_value))
        reduce_output_partitions.append(reduce_output)

    return reduce_output_partitions
