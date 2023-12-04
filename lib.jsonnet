local yaml = importstr 'config.yaml';
local config = std.parseYaml(yaml);

local maybe(cond, obj) = if cond then obj else null;
local cmd(cmd) = 'cargo run --release -- ' + cmd;
local pipeline(stages, flag=true) = { stages: if flag then stages else {} };
local normalizePath(path) =
  local parts = std.split(path, '/');
  local norm(ps, pfx) =
    if std.length(ps) == 0
    then pfx
    else if ps[0] == '.' || ps[0] == ''
    then norm(ps[1:], pfx)
    else if ps[0] == '..'
    then norm(ps[1:], pfx[0:std.length(pfx) - 1])
    else norm(ps[1:], pfx + [ps[0]]);
  std.join('/', norm(parts, []));

// get the outputs of a stage.
// dir: the directory of the file defining the stage
// stage: the stage definition
local stageOuts(dir, stage) =
  [
    normalizePath(std.join('/', [dir, std.get(stage, 'wdir', '.'), out]))
    for out in stage.outs
  ];

{
  config: config,
  maybe: maybe,
  cmd: cmd,
  pipeline: pipeline,
  stageOuts: stageOuts,
}
