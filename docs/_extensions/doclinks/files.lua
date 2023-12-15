function bd_load_filemap()
    local path, f, text
    if bd_files == nil then
        path = pandoc.path.join({ quarto.project.output_directory, "files.json" })
        f = io.open(path)
        if f == nil then
            quarto.log.warning("could not open file list")
            files = {}
            return
        end
        text = f:read("a")
        bd_files = quarto.json.decode(text)
    end
end

return {
    ['file'] = function(args, kwargs, meta)
        local path, fn, name, link, pat, sub, m, ms
        bd_load_filemap()
        path = pandoc.utils.stringify(args[1])
        quarto.log.debug('finding link for', path)
        local target = bd_files[path]

        if target == nil then
            return pandoc.Strong({
                "{{ERR unknown file ",
                pandoc.Code(path),
                "}}"
            })
        else
            local link = "/" .. target.page .. "#file:" .. path
            return pandoc.Link(pandoc.Code(path), link)
        end
    end,
}
