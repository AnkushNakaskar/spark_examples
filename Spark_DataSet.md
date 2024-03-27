# FAQ on spark dataset
 * Dataset is Dataframe of type 'Row', Dataset is only supported in Scala/Java
 * To map Java class datatype <> spark internal datatype, encoder is used.
 * Dataset has performance hit than dataframe since conversion will take time.
   * When the operation(s) you would like to perform cannot be expressed using DataFrame manipulations
   * When you want or need type-safety, and you’re willing to accept the cost of performance to achieve it
