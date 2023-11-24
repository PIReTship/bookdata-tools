local yaml = importstr 'config.yaml';
local config = std.parseYaml(yaml);

local cmd(cmd) = 'cargo run --release -- ' + cmd;
local pipeline(stages, flag=true) = { stages: if flag then stages else {} };
// get the outputs of a stage. only works w/ 1 subdir level,
// which is all we need in this project
// dir: the directory of the file defining the stage
// stage: the stage definition
local stageOuts(dir, stage) =
  if std.get(stage, 'wdir', '.') == '..'
  then stage.outs
  else [
    dir + '/' + out
    for out in stage.outs
  ];

{
  config: config,
  cmd: cmd,
  pipeline: pipeline,
  stageOuts: stageOuts,
}
