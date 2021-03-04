//This Macro requires the MultiStackReg plug in (tested with version 1.45)
//The TurboReg plug in is also required, but may be packaged with recent 
//versions of Fiji

setBatchMode(true);

argv = getArgument();
argArray = split(argv, ":");
if(argArray.length != 5 && argArray.length != 6) {
    exit("expected six colon separated arguments like: \"background_in:background_out:glomeruli_in:glomeruli_out:transformation_file_out[:transformation_type]\"");
}

if (argArray.length == 5) {
    transformation = "Rigid Body";
}
else {
    transformation = argArray[5];
}

background_in = argArray[0];
background_out = argArray[1];
gomeruli_in = argArray[2];
gomeruli_out = argArray[3];
transformation_out = argArray[4];

open(background_in);
background_title = getTitle();

run("MultiStackReg", "stack_1=" + background_title + " action_1=Align file_1=[" + transformation_out + "] stack_2=None action_2=Ignore file_2=[] transformation=[" +  transformation + "] save");

save(background_out);
close();

open(gomeruli_in);
gomeruli_title = getTitle();

run("MultiStackReg", "stack_1=" + gomeruli_title + " action_1=[Load Transformation File] file_1=[" + transformation_out + "] stack_2=None action_2=Ignore file_2=[] transformation=[" +  transformation + "] ");

save(gomeruli_out);
close();