function rust_parse_ref(name)
    local tpath, tname, label
    tpath, tname = string.match(name, "^~(.*::([^:]+))")
    if tpath then
        label = pandoc.Code(tname)
    else
        tpath = name
        label = pandoc.Code(name)
    end
    return tpath, label
end

function rust_link(arg, type)
    local name, path, label, tgt
    name = pandoc.utils.stringify(arg)
    path, label = rust_parse_ref(name)
    tgt = "/apidocs/" .. string.gsub(path, "::", "/")
    if type == "mod" then
        tgt = tgt .. "/"
    else
        tgt = string.gsub(tgt, "/([^/]*)$", "/" .. type .. ".%1.html")
    end
    return pandoc.Link(label, tgt)
end

return {
    ['rust-fn'] = function(args, kwargs, meta)
        return rust_link(args[1], "fn")
    end,
    ['rust-struct'] = function(args, kwargs, meta)
        return rust_link(args[1], "struct")
    end,
    ['rust-mod'] = function(args, kwargs, meta)
        return rust_link(args[1], "mod")
    end
  }
