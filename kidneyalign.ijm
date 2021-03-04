// Macro for aligning .tif stacks for Kidney reconstructions the first time
//
// tested with fiji versions from
// http://jenkins.imagej.net/job/Stable-Fiji/lastSuccessfulBuild/artifact/fiji-nojre.zip
// for linux and
// http://jenkins.imagej.net/job/Stable-Fiji-MacOSX/lastSuccessfulBuild/artifact/fiji-macosx.dmg
// for OS X
//
// Can be run on mac using something like:
//   /Applications/Fiji.app/Contents/MacOS/ImageJ-macosx -batch kidneyalign.ijm "data/Test_stack_01.tif:test1out.tif"
// and on linux like:
//   $FIJI_HOME_DIR/ImageJ-linux64 -batch kidneyalign.ijm "data/Test_stack_02.tif:test2out.tif"

setBatchMode(true);
argv = getArgument();
argArray = split(argv, ":");
if(argArray.length != 2) {
    exit("expected two colon separated arguments like: \"input_file:output_file\"");
}

origin = argArray[0];
result = argArray[1];
open(origin);
run("Canvas Size...", "width=3000 height=2000 position=Center");
run("Macro...", "code=[if (v<1) v=255] stack");
run("StackReg", "transformation=[Rigid Body]");
run("Macro...", "code=[if (v<1) v=255] stack");
saveAs(".tiff", result);
close();

