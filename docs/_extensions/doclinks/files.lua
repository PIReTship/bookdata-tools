return {
    ['file'] = function(args, kwargs, meta)
        local path, dir, name, link, pos
        path = pandoc.utils.stringify(args[1])
        pos = string.find(path, "/", 1, true)
        if pos ~= nil then
            dir = string.sub(path, 1, pos)
            name = string.sub(path, pos + 1)
            link = "/data/" .. string.sub(dir, 1, -2) .. ".qmd#file:" .. name
        else
            link = "#file:" .. path
        end
        return pandoc.Link(pandoc.Code(path), link)
    end,
}
